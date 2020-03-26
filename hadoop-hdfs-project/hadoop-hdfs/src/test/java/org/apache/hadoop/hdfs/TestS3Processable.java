package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.S3File;
import org.apache.hadoop.hdfs.protocol.S3Object;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_ENABLED_KEY;

public class TestS3Processable {
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private NamenodeProtocols nn;
  private Configuration conf;

  private final static int fileSize = 64;
  private final static int appendSize = 32;
  private final static String fileName = "s3file.dat";
  private final static String fileName1 = "s3file1.dat";
  private final static String fileName2 = "s3file2.dat";
  private final static byte replication = 1;
  private final static int defaultBufferSize = 1024;
  private final static long seed = 0xDEADBEEFL;
  private final static int blockSize = 2048;

  @Test(timeout = 40_000)
  public void testAppendProcessable() throws IOException {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY, true);
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).checkDataNodeHostConfig(true).build();
    fs = cluster.getFileSystem();
    nn = cluster.getNameNodeRpc();

    try {
      Path file = new Path(fileName);
      Path parent = file.getParent();
      fs.mkdirs(parent);

      byte[] fileContent = AppendTestUtil.randomBytes(seed, fileSize);

      // Write
      FSDataOutputStream stm = fs.create(file, true, fs.getConf()
              .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
      stm.write(fileContent, 0, fileSize);
      stm.close();
      fs.deleteOnExit(file);

      // Append
      byte[] appendContent = AppendTestUtil.randomBytes(seed, appendSize);
      stm = fs.append(file);
      stm.write(appendContent, 0, appendSize);
      stm.close();

      int count = 5;
      while(count > 0) {
        count--;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      cluster.shutdown();
    }
  }

  @Test(/*timeout = 40_000*/)
  public void testRenameDirProcessables() throws IOException {
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY, true);
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).checkDataNodeHostConfig(true).build();
    fs = cluster.getFileSystem();
    nn = cluster.getNameNodeRpc();

    try {
      Path dir = new Path("s3proc_original");
      fs.mkdirs(dir);
      Path file = new Path(dir, fileName);

      byte[] fileContent = AppendTestUtil.randomBytes(seed, fileSize);

      // Write
      FSDataOutputStream stm = fs.create(file, true, fs.getConf()
              .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
      stm.write(fileContent, 0, fileSize);
      stm.close();

      // Rename directory
      Path newDirName = new Path("s3proc_changed");
      fs.rename(dir, newDirName);

      int count = 7;
      while(count > 0) {
        count--;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      fs.delete(newDirName, true);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      cluster.shutdown();
    }
  }

  private List<S3Object> getS3ObjectsForPath(Path path) throws IOException {
    S3File s3File = nn.getS3File(fixRelativePart(path).toUri().getPath(), 0, 0); // TODO FCG fix after ranged query supported
    return s3File.getObjects();
  }

  private Path fixRelativePart(Path p) {
    if (p.isUriPathAbsolute()) {
      return p;
    } else {
      return new Path(fs.getWorkingDirectory(), p);
    }
  }

  static boolean equalContent(byte[] buf1, int off1, int len1, byte[] buf2, int off2, int len2) {
    return ByteBuffer.wrap(buf1, off1, len1).equals(ByteBuffer.wrap(buf2, off2, len2));
  }

  static byte[] append(byte[] buf1, int len1, byte[] buf2, int len2) {
    byte[] result = new byte[len1 + len2];
    System.arraycopy(buf1, 0, result, 0, len1);
    System.arraycopy(buf2, 0, result, len1, len2);
    return result;
  }
}
