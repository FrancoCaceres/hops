package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestS3Truncate {
  static final long seed = 0xDEADBEEFL;
  static final int fileSize = 128;
  static final String fileName = "s3file.dat";
  static final int blockSize = 2048;
  static final byte replication = 1;
  static final int defaultBufferSize = 1024;

  @Test
  public void testTruncateSingleObject() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).checkDataNodeHostConfig(true).build();
    FileSystem fs = cluster.getFileSystem();

    try {
      Path file1 = new Path(fileName);
      Path parent = file1.getParent();
      fs.mkdirs(parent);

      byte[] fileContent = AppendTestUtil.randomBytes(seed, fileSize);

      // Write
      FSDataOutputStream stm = fs.create(file1, true, fs.getConf()
              .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
      stm.write(fileContent, 0, fileSize);
      stm.close();
      fs.deleteOnExit(file1);

      // Truncate to half
      fs.truncate(file1, fileSize / 2);

      // Read whole
      FSDataInputStream fis = fs.open(file1, 1024);
      byte[] readContent = new byte[fileSize / 2];
      int bytesRead = fis.read(readContent, 0, fileSize);
      assertEquals("Number of bytes read should be " + fileSize / 2 + " , but is " + bytesRead,
              fileSize / 2, bytesRead);
      assertTrue("The bytes read should be equal to the content of the file",
              equalContent(readContent, 0, bytesRead, fileContent, 0, fileSize / 2));

      // Truncate to empty
      fs.truncate(file1, 0);
      bytesRead = fis.read(readContent, 0, fileSize);
      assertEquals("Number of bytes read should be " + -1 + " , but is " + bytesRead, -1,
              bytesRead);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testTruncateMutlipleObject() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).checkDataNodeHostConfig(true).build();
    FileSystem fs = cluster.getFileSystem();

    try {
      Path file1 = new Path(fileName);
      Path parent = file1.getParent();
      fs.mkdirs(parent);

      byte[] fileContent = AppendTestUtil.randomBytes(seed, fileSize);

      // Write
      FSDataOutputStream stm = fs.create(file1, true, fs.getConf()
              .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
      stm.write(fileContent, 0, fileSize);
      stm.close();
      fs.deleteOnExit(file1);

      // Append
      int appendSize = 56;
      byte[] appendContent = AppendTestUtil.randomBytes(seed, appendSize);
      stm = fs.append(file1);
      stm.write(appendContent, 0, appendSize);
      stm.close();

      int totalSize = fileSize + appendSize;
      byte[] totalContent = append(fileContent, fileSize, appendContent, appendSize);

      // Append again
      appendSize = 56;
      appendContent = AppendTestUtil.randomBytes(seed, appendSize);
      stm = fs.append(file1);
      stm.write(appendContent, 0, appendSize);
      stm.close();

      totalContent = append(totalContent, totalSize, appendContent, appendSize);
      totalSize += appendSize;

      // Truncate
      int newSize = totalSize - appendSize / 2;
      fs.truncate(file1, newSize);

      // Read whole
      FSDataInputStream fis = fs.open(file1, 1024);
      byte[] readContent = new byte[newSize];
      int bytesRead = fis.read(readContent, 0, newSize * 2); // Attempt to read more than needed
      assertEquals("Number of bytes read should be " + newSize + " , but is " + bytesRead,
              newSize, bytesRead);
      assertTrue("The bytes read should be equal to the content of the file",
              equalContent(readContent, 0, bytesRead, totalContent, 0, newSize));

      // Truncate
      newSize = newSize - appendSize ;
      fs.truncate(file1, newSize);

      // Read whole
      fis = fs.open(file1, 1024);
      readContent = new byte[newSize];
      bytesRead = fis.read(readContent, 0, newSize * 2); // Attempt to read more than needed
      assertEquals("Number of bytes read should be " + newSize + " , but is " + bytesRead,
              newSize, bytesRead);
      assertTrue("The bytes read should be equal to the content of the file",
              equalContent(readContent, 0, bytesRead, totalContent, 0, newSize));

      // Truncate
      newSize = newSize - appendSize ;
      fs.truncate(file1, newSize);

      // Read whole
      fis = fs.open(file1, 1024);
      readContent = new byte[newSize];
      bytesRead = fis.read(readContent, 0, newSize * 2); // Attempt to read more than needed
      assertEquals("Number of bytes read should be " + newSize + " , but is " + bytesRead,
              newSize, bytesRead);
      assertTrue("The bytes read should be equal to the content of the file",
              equalContent(readContent, 0, bytesRead, totalContent, 0, newSize));

      // Truncate to empty
      fs.truncate(file1, 0);
      bytesRead = fis.read(readContent, 0, fileSize);
      assertEquals("Number of bytes read should be " + -1 + " , but is " + bytesRead, -1,
              bytesRead);
    } finally {
      cluster.shutdown();
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
