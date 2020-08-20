package org.dcache.nfs.v4.nio2;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.dcache.nfs.nfsstat;
import org.dcache.nfs.v4.AttributeMap;
import org.dcache.nfs.v4.client.CompoundBuilder;
import org.dcache.nfs.v4.client.nfs4_prot_NFS4_PROGRAM_Client;
import org.dcache.nfs.v4.xdr.COMPOUND4args;
import org.dcache.nfs.v4.xdr.COMPOUND4res;
import org.dcache.nfs.v4.xdr.clientid4;
import org.dcache.nfs.v4.xdr.fattr4_lease_time;
import org.dcache.nfs.v4.xdr.nfs4_prot;
import org.dcache.nfs.v4.xdr.nfs_fh4;
import org.dcache.nfs.v4.xdr.nfs_opnum4;
import org.dcache.nfs.v4.xdr.nfstime4;
import org.dcache.nfs.v4.xdr.sequenceid4;
import org.dcache.nfs.v4.xdr.sessionid4;
import org.dcache.nfs.v4.xdr.state_protect_how4;
import org.dcache.oncrpc4j.rpc.OncRpcException;
import org.dcache.oncrpc4j.rpc.net.IpProtocolType;

/**
 *
 */
public class NfsFileSystem extends FileSystem {

    private final nfs4_prot_NFS4_PROGRAM_Client nfsClient;
    private clientid4 _clientIdByServer = null;
    private sequenceid4 _sequenceID = null;
    private sessionid4 _sessionid = null;
    private long lastUpdate;
    private int slotId = -1;
    private Path root;
    private final FileSystemProvider provider;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    NfsFileSystem(FileSystemProvider provider,  URI server) throws IOException {
        this.provider = provider;
        HostAndPort hp = HostAndPort.fromString(server.getHost())
                .withDefaultPort(2049)
                .requireBracketsForIPv6();

        InetSocketAddress address = new InetSocketAddress(hp.getHost(), hp.getPort());
        nfsClient = new nfs4_prot_NFS4_PROGRAM_Client(address.getAddress(),
                address.getPort(), IpProtocolType.TCP);

        exchange_id();
        create_session();
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
        return nfsClient.getTransport().isOpen();
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

        COMPOUND4args args = new CompoundBuilder()
                .withExchangeId("", NfsFileSystem.class.getName(), UUID.randomUUID().toString(), 0, state_protect_how4.SP4_NONE)
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

    private COMPOUND4res sendCompound(COMPOUND4args compound4args)
            throws OncRpcException, IOException {

        COMPOUND4res compound4res;
        /*
         * wail if server is in the grace period.
         *
         * TODO: escape if it takes too long
         */
        do {
            compound4res = nfsClient.NFSPROC4_COMPOUND_4(compound4args);
            processSequence(compound4res);
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

        _sessionid = compound4res.resarray.get(0).opcreate_session.csr_resok4.csr_sessionid;
        _sequenceID.value = 0;
        slotId = compound4res.resarray.get(0).opcreate_session.csr_resok4.csr_fore_chan_attrs.ca_maxrequests.value - 1;

        args = new CompoundBuilder()
                .withSequence(false, _sessionid, _sequenceID.value, slotId, 0)
                .withPutrootfh()
                .withGetattr(nfs4_prot.FATTR4_LEASE_TIME)
                .withTag("get_lease_time")
                .build();

            compound4res = sendCompound(args);

            AttributeMap attributeMap = new AttributeMap(compound4res.resarray.get(compound4res.resarray.size() - 1).opgetattr.resok4.obj_attributes);
            Optional<fattr4_lease_time> fattr4_lease_timeAttr = attributeMap.get(nfs4_prot.FATTR4_LEASE_TIME);
            int leaseTimeInSeconds = fattr4_lease_timeAttr.get().value;

        executorService.scheduleAtFixedRate(() -> {
            try {
                sequence();
            } catch (IOException e) {
            }
        },
                 leaseTimeInSeconds, leaseTimeInSeconds, TimeUnit.SECONDS
        );
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
        nfsClient.close();

    }

    private void getRootFh(String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withSequence(false, _sessionid, _sequenceID.value, slotId, 0)
                .withPutrootfh()
                .withLookup(path)
                .withGetfh()
                .withTag("get_rootfh")
                .build();

        COMPOUND4res compound4res = sendCompound(args);

        nfs_fh4 fh = compound4res.resarray.get(compound4res.resarray.size() - 1).opgetfh.resok4.object;
        root = new NfsPath(this, fh, null, null, null);
    }

    private void sequence() throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withSequence(false, _sessionid, _sequenceID.value, slotId, 0)
                .withTag("sequence")
                .build();
        COMPOUND4res compound4res = sendCompound(args);
    }

    nfs_fh4 lookup(nfs_fh4 fh, String path) throws OncRpcException, IOException {

        COMPOUND4args args = new CompoundBuilder()
                .withSequence(false, _sessionid, _sequenceID.value, slotId, 0)
                .withPutfh(fh)
                .withLookup(path)
                .withGetfh()
                .withTag("lookup")
                .build();

        COMPOUND4res compound4res = sendCompound(args);
        return compound4res.resarray.get(compound4res.resarray.size() - 1).opgetfh.resok4.object;
    }

    public void processSequence(COMPOUND4res compound4res) {

        if (compound4res.resarray.get(0).resop == nfs_opnum4.OP_SEQUENCE && compound4res.resarray.get(0).opsequence.sr_status == nfsstat.NFS_OK) {
            lastUpdate = System.currentTimeMillis();
            ++_sequenceID.value;
        }
    }
}
