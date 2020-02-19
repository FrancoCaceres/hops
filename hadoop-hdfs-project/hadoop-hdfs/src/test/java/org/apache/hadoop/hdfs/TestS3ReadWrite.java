package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY;
import static org.junit.Assert.assertTrue;

public class TestS3ReadWrite {
  static final long seed = 0xDEADBEEFL;
  static final int fileSize = 256;
  static final String fileName = "s3file.dat";
  static final int blockSize = 4096;
  static final byte replication = 1;
  static final int defaultBufferSize = 4096;

  @Test
  public void testReadAfterWrite() throws IOException {
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
      assertTrue(file1 + " should be a file", fs.getFileStatus(file1).isFile());
      stm.write(fileContent, 0, fileSize);
      stm.close();
      fs.deleteOnExit(file1);

      // Check file status
      long len = fs.getFileStatus(file1).getLen();
      assertTrue(file1 + " should be of size " + fileSize +
              " but found to be of size " + len, len == fileSize);

      // Read whole
      FSDataInputStream fis = fs.open(file1, 1024);
      byte[] readContent = new byte[2500];
      int bytesRead = fis.read(readContent, 0, fileSize);
      assertTrue("Number of bytes read should be " + fileSize + " , but is " + bytesRead,
              bytesRead == fileSize);
      assertTrue("The bytes read should be equal to the content of the file",
              ByteBuffer.wrap(fileContent, 0, fileSize).equals(ByteBuffer.wrap(readContent, 0, fileSize)));

      // Read range
      bytesRead = fis.read(128, readContent, 0, 128);
      assertTrue(bytesRead == 128);
      assertTrue(ByteBuffer.wrap(fileContent, 128, 128).equals(ByteBuffer.wrap(readContent, 0, 128)));

      // Read more than remaining
      fis.seek(0);
      bytesRead = fis.read(readContent, 0, fileSize + 128);
      assertTrue(bytesRead == fileSize);
      assertTrue(ByteBuffer.wrap(fileContent, 0, fileSize).equals(ByteBuffer.wrap(readContent, 0, fileSize)));

      // Read after EOF
      bytesRead = fis.read(readContent, 0, fileSize);
      assertTrue(bytesRead == -1);
    } finally {
      cluster.shutdown();
    }
  }
}
