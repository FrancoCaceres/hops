package org.apache.hadoop.hdfs;

import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.S3ObjectDeletableDataAccess;
import io.hops.metadata.hdfs.entity.S3ObjectDeletable;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.S3File;
import org.apache.hadoop.hdfs.protocol.S3Object;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY;
import static org.junit.Assert.*;

public class TestS3Delete {
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private NamenodeProtocols nn;

  private final static Configuration conf;
  private final static int fileSize = 64;
  private final static String fileName1 = "s3file1.dat";
  private final static String fileName2 = "s3file2.dat";
  private final static byte replication = 1;
  private final static int defaultBufferSize = 1024;
  private final static long seed = 0xDEADBEEFL;
  private final static int blockSize = 2048;

  static {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY, true);
  }

  @Before
  public void startUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).checkDataNodeHostConfig(true).build();
    assertNotNull("Failed Cluster Creation", cluster);
    fs = cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", fs);
    nn = cluster.getNameNodeRpc();
    assertNotNull("Failed to get NameNode", nn);
  }

  @After
  public void shutDownCluster() throws IOException {
    if(fs != null) {
      fs.close();
    }
    if(cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSingleFile() throws IOException {
    Path file = new Path(fileName1);
    Path parent = file.getParent();
    fs.mkdirs(parent);

    byte[] fileContent = AppendTestUtil.randomBytes(seed, fileSize);

    // Write
    FSDataOutputStream stm = fs.create(file, true, fs.getConf()
            .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
    stm.write(fileContent, 0, fileSize);
    stm.close();

    // Check that file exists
    assertTrue(file + " should be a file", fs.getFileStatus(file).isFile());
    List<S3Object> objs = getS3ObjectsForPath(file);
    // Delete
    fs.delete(file, false);

    // Check that file does not exist
    assertFalse(file + " should not exist", fs.exists(file));

    // Check that a deletable has been created for each S3 object
    List<S3ObjectDeletable> deletables = getAllDeletables();

    assertTrue("Deletables not found for file's S3 objects", deletablesExist(objs, deletables));
  }

  @Test
  public void testDirectory() throws IOException {
    Path dir = new Path("/user/franco/test");
    fs.mkdirs(dir);

    Path file1 = new Path(dir, fileName1);
    Path file2 = new Path(dir, fileName2);

    byte[] file1Content = AppendTestUtil.randomBytes(seed, fileSize);
    byte[] file2Content = AppendTestUtil.randomBytes(seed, fileSize);

    // Write
    FSDataOutputStream stm = fs.create(file1, true, fs.getConf()
            .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
    stm.write(file1Content, 0, fileSize);
    stm.close();
    stm = fs.create(file2, true, fs.getConf()
            .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
    stm.write(file2Content, 0, fileSize);
    stm.close();

    // Check that files exist
    assertTrue(file1 + " should be a file", fs.getFileStatus(file1).isFile());
    assertTrue(file2 + " should be a file", fs.getFileStatus(file2).isFile());
    List<S3Object> objs1 = getS3ObjectsForPath(file1);
    List<S3Object> objs2 = getS3ObjectsForPath(file2);

    // Delete directory
    fs.delete(dir, true);

    // Check that files do not exist
    assertFalse(file1 + " should not exist", fs.exists(file1));
    assertFalse(file2 + " should not exist", fs.exists(file2));

    // Check that a deletable has been created for each S3 object
    List<S3ObjectDeletable> deletables = getAllDeletables();

    assertTrue("Deletables not found for file1's S3 objects", deletablesExist(objs1, deletables));
    assertTrue("Deletables not found for file2's S3 objects", deletablesExist(objs2, deletables));
  }

  private List<S3ObjectDeletable> getAllDeletables() throws IOException {
    return (List<S3ObjectDeletable>) new LightWeightRequestHandler(HDFSOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        S3ObjectDeletableDataAccess da =
                (S3ObjectDeletableDataAccess) HdfsStorageFactory.getDataAccess(S3ObjectDeletableDataAccess.class);
        return da.getAll();
      }
    }.handle();
  }

  private List<S3Object> getS3ObjectsForPath(Path path) throws IOException {
    S3File s3File = nn.getS3File(fixRelativePart(path).toUri().getPath(), 0, 0); // TODO FCG fix after ranged query supported
    return s3File.getObjects();
  }

  private boolean deletablesExist(List<S3Object> objs, List<S3ObjectDeletable> deletables) {
    for(S3Object obj : objs) {
      boolean found = false;
      for(S3ObjectDeletable del : deletables) {
        if(deletableForObject(obj, del)) {
          found = true;
          break;
        }
      }
      if(!found) {
        return false;
      }
    }
    return true;
  }

  private boolean deletableForObject(S3Object obj, S3ObjectDeletable del) {
    return obj.getRegion().equals(del.getRegion())
            && obj.getBucket().equals(del.getBucket())
            && obj.getKey().equals(del.getKey())
            && obj.getVersionId().equals(del.getVersionId());
  }

  private Path fixRelativePart(Path p) {
    if (p.isUriPathAbsolute()) {
      return p;
    } else {
      return new Path(fs.getWorkingDirectory(), p);
    }
  }
}
