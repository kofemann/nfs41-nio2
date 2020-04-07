package org.dcache.nfs.v4.nio2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tigran
 */
public class NfsFileSystemProviderTest {

    private FileSystem fs;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        fs = FileSystems.newFileSystem(new URI("nfs://dcache-lab007/exports/data"), Collections.emptyMap(), Thread.currentThread().getContextClassLoader());
    }

    @Test
    public void testResolve() {
        Path p = fs.getPath("desy.de", "belle");
        System.out.println(p);
    }

    @After
    public void tearDown() throws IOException {
        fs.close();
    }
}
