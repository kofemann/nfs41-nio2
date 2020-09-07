package org.dcache.nfs.v4.nio2;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.util.UnixUtils;
import org.dcache.nfs.v4.AttributeMap;
import org.dcache.nfs.v4.ClientSession;
import org.dcache.nfs.v4.CompoundBuilder;
import org.dcache.nfs.v4.xdr.COMPOUND4args;
import org.dcache.nfs.v4.xdr.COMPOUND4res;
import org.dcache.nfs.v4.xdr.SEQUENCE4args;
import org.dcache.nfs.v4.xdr.clientid4;
import org.dcache.nfs.v4.xdr.entry4;
import org.dcache.nfs.v4.xdr.fattr4_lease_time;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_argop4;
import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfstime4;
import org.dcache.nfs.v4.xdr.sequenceid4;
import org.dcache.nfs.v4.xdr.sessionid4;
import org.dcache.nfs.v4.xdr.slotid4;
import org.dcache.nfs.v4.xdr.state_protect_how4;
import org.dcache.nfs.v4.xdr.verifier4;
import org.dcache.oncrpc4j.rpc.OncRpcClient;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.rpc.RpcAuth;
import org.dcache.oncrpc4j.rpc.RpcAuthTypeUnix;
import org.dcache.oncrpc4j.rpc.RpcCall;
import org.dcache.oncrpc4j.rpc.RpcTransport;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;

/**
 *
 */
public class NfsFileSystem extends FileSystem {

    private final RpcCall client;
    private final OncRpcClient rpcClient;

    private clientid4 _clientIdByServer = null;
    private sequenceid4 _sequenceID = null;
    private sessionid4 _sessionid = null;
    private long lastUpdate;
    private NfsPath root;
    private final FileSystemProvider provider;
    private ClientSession clientSession;

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    NfsFileSystem(FileSystemProvider provider,  URI server) throws IOException {
        this.provider = provider;
        HostAndPort hp = HostAndPort.fromString(server.getHost())
                .withDefaultPort(2049)
                .requireBracketsForIPv6();


        Subject currentUser = UnixUtils.getCurrentUser();
        if (currentUser == null) {
            throw new IllegalStateException("unable to determine current unix user. please provide uid/gid explicitly");
        }

        int uid = (int) Subjects.getUid(currentUser);
        int gid = (int) Subjects.getPrimaryGid(currentUser);
        int[] gids = UnixUtils.toIntArray(Subjects.getGids(currentUser));

        InetSocketAddress address = new InetSocketAddress(hp.getHost(), hp.getPort());
        rpcClient = new OncRpcClient(address.getAddress(), IpProtocolType.TCP, address.getPort());
        RpcTransport transport;
        transport = rpcClient.connect();

        RpcAuth credential = new RpcAuthTypeUnix(uid, gid, gids,
                (int) Instant.now().getEpochSecond(),
                InetAddress.getLocalHost().getHostName());
        client = new RpcCall(100003, 4, credential, transport);

        exchange_id();
        create_session();
        reclaim_complete();
        getRootFh(server.getPath());
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        destroy_session();
        destroy_clientid();
    }

    @Override
    public boolean isOpen() {
        return client.getTransport().isOpen();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return ImmutableSet.of("basic", "posix");
    }

    @Override
    public Path getPath(String first, String... more) {

        Path p = root.resolve(first);
        for (String s : more) {
            p = p.resolve(s);
        }
        return p;
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void exchange_id() throws OncRpcException, IOException {

        String id = this.getClass().getCanonicalName() + ": "
                + ProcessHandle.current().info().user().orElse("<nobody>")
                + "-"
                + ProcessHandle.current().pid() + "@" + InetAddress.getLocalHost().getHostName();

        COMPOUND4args args = new CompoundBuilder()
                .withExchangeId(this.getClass().getPackageName(), NfsFileSystem.class.getName(), id, 0, state_protect_how4.SP4_NONE)
                .withTag("exchange_id")
                .build();

        COMPOUND4res compound4res = sendCompound(args);

        if (compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_server_impl_id.length > 0) {
            String serverId = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_server_impl_id[0].nii_name.toString();
            nfstime4 buildTime = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_server_impl_id[0].nii_date;
            System.out.println("Connected to: " + serverId + ", built at: "
                    + (buildTime.seconds > 0 ? new Date(buildTime.seconds * 1000) : "<Unknon>"));
        } else {
            System.out.println("Connected to: Mr. X");
        }

        _clientIdByServer = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_clientid;
        _sequenceID = compound4res.resarray.get(0).opexchange_id.eir_resok4.eir_sequenceid;

    }

    private COMPOUND4res sendCompoundInSession(COMPOUND4args compound4args)
            throws OncRpcException, IOException {

        if (compound4args.argarray[0].argop == nfs_opnum4.OP_SEQUENCE) {
            throw new IllegalArgumentException();
        }

        nfs_argop4[] extendedOps = new nfs_argop4[compound4args.argarray.length + 1];
        System.arraycopy(compound4args.argarray, 0, extendedOps, 1, compound4args.argarray.length);
        compound4args.argarray = extendedOps;

        ClientSession.SessionSlot slot = clientSession.acquireSlot();
        try {

            COMPOUND4res compound4res = new COMPOUND4res();
            /*
             * wait if server is in the grace period.
             *
             * TODO: escape if it takes too long
             */
            do {

                nfs_argop4 op = new nfs_argop4();
                op.argop = nfs_opnum4.OP_SEQUENCE;
                op.opsequence = new SEQUENCE4args();
                op.opsequence.sa_cachethis = false;

                op.opsequence.sa_slotid = slot.getId();
                op.opsequence.sa_highest_slotid = new slotid4(clientSession.maxRequests() - 1);
                op.opsequence.sa_sequenceid = slot.nextSequenceId();
                op.opsequence.sa_sessionid = clientSession.sessionId();

                compound4args.argarray[0] = op;

                client.call(nfs4_prot.NFSPROC4_COMPOUND_4, compound4args, compound4res);
                lastUpdate = System.currentTimeMillis();

                if (compound4res.status == nfsstat.NFSERR_GRACE) {
                    System.out.println("Server in GRACE period....retry");
                }
            } while (compound4res.status == nfsstat.NFSERR_GRACE);

            nfsstat.throwIfNeeded(compound4res.status);
            return compound4res;
        } finally {
            clientSession.releaseSlot(slot);
        }
    }

    private COMPOUND4res sendCompound(COMPOUND4args compound4args)
            throws OncRpcException, IOException {

        COMPOUND4res compound4res = new COMPOUND4res();
        /*
         * wait if server is in the grace period.
         *
         * TODO: escape if it takes too long
         */
        do {
            client.call(nfs4_prot.NFSPROC4_COMPOUND_4, compound4args, compound4res);
            if (compound4res.status == nfsstat.NFSERR_GRACE) {
                System.out.println("Server in GRACE period....retry");
            }
        } while (compound4res.status == nfsstat.NFSERR_GRACE);

        nfsstat.throwIfNeeded(compound4res.status);
        return compound4res;
    }

    private void create_session() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withCreatesession(_clientIdByServer, _sequenceID)
                .withTag("create_session")
                .build();

        COMPOUND4res compound4res = sendCompound(args);

        _sequenceID.value++;
        _sessionid = compound4res.resarray.get(0).opcreate_session.csr_resok4.csr_sessionid;
        int maxRequests = compound4res.resarray.get(0).opcreate_session.csr_resok4.csr_fore_chan_attrs.ca_maxrequests.value;
        clientSession = new ClientSession(_sessionid, maxRequests);

        args = new CompoundBuilder()
                .withPutrootfh()
                .withGetattr(nfs4_prot.FATTR4_LEASE_TIME)
                .withTag("get_lease_time")
                .build();

        compound4res = sendCompoundInSession(args);

        AttributeMap attributeMap = new AttributeMap(compound4res.resarray.get(compound4res.resarray.size() - 1).opgetattr.resok4.obj_attributes);
        Optional<fattr4_lease_time> fattr4_lease_timeAttr = attributeMap.get(nfs4_prot.FATTR4_LEASE_TIME);
        int leaseTimeInSeconds = fattr4_lease_timeAttr.get().value;

        executorService.scheduleAtFixedRate(() -> {
            try {
                sendCompoundInSession(new CompoundBuilder()
                        .withTag("renew")
                        .build());
            } catch (IOException e) {
            }
        },
                 leaseTimeInSeconds, leaseTimeInSeconds, TimeUnit.SECONDS
        );
    }

    private void reclaim_complete() throws IOException {
        COMPOUND4args args = new CompoundBuilder()
                .withReclaimComplete()
                .withTag("reclaim_complete")
                .build();

        @SuppressWarnings("unused")
        COMPOUND4res compound4res = sendCompoundInSession(args);
    }

    private void destroy_session() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withDestroysession(_sessionid)
                .withTag("destroy_session")
                .build();

        @SuppressWarnings("unused")
        COMPOUND4res compound4res = sendCompound(args);
        executorService.shutdown();
    }

    private void destroy_clientid() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withDestroyclientid(_clientIdByServer)
                .withTag("destroy_clientid")
                .build();
        @SuppressWarnings("unused")
        COMPOUND4res compound4res = sendCompound(args);
        rpcClient.close();

    }

    private void getRootFh(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutrootfh()
                .withLookup(path)
                .withGetfh()
                .withTag("get_rootfh")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);

        nfs_fh4 fh = compound4res.resarray.get(compound4res.resarray.size() - 1).opgetfh.resok4.object;
        root = new NfsPath(this, fh, null, null, null);
    }

    nfs_fh4 lookup(nfs_fh4 fh, String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withPutfh(fh)
                .withLookup(path)
                .withGetfh()
                .withTag("lookup")
                .build();

        COMPOUND4res compound4res = sendCompoundInSession(args);
        return compound4res.resarray.get(compound4res.resarray.size() - 1).opgetfh.resok4.object;
    }


    Path[] list(NfsPath dir) throws OncRpcException, IOException, ChimeraNFSException {

        boolean done;
        List<Path> list = new ArrayList<>();
        long cookie = 0;
        verifier4 verifier = new verifier4(new byte[nfs4_prot.NFS4_VERIFIER_SIZE]);

        do {

            COMPOUND4args args = new CompoundBuilder()
                    .withPutfh(dir.fh())
                    .withReaddir(cookie, verifier, 16384, 16384, nfs4_prot.FATTR4_FILEHANDLE)
                    .withTag("readdir")
                    .build();

            COMPOUND4res compound4res = sendCompoundInSession(args);

            verifier = compound4res.resarray.get(2).opreaddir.resok4.cookieverf;
            done = compound4res.resarray.get(2).opreaddir.resok4.reply.eof;

            entry4 dirEntry = compound4res.resarray.get(2).opreaddir.resok4.reply.entries;
            while (dirEntry != null) {
                cookie = dirEntry.cookie.value;

                AttributeMap attrs = new AttributeMap(dirEntry.attrs);
                Optional<nfs_fh4> fh = attrs.get(nfs4_prot.FATTR4_FILEHANDLE);
                list.add(new NfsPath(this, fh.get(), root, dir, new String(dirEntry.name.value, StandardCharsets.UTF_8)));
                dirEntry = dirEntry.nextentry;
            }

        } while (!done);

        return list.toArray(new Path[list.size()]);
    }
}
