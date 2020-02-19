package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY;
import static org.junit.Assert.assertTrue;

public class TestS3Concat {
  static final long seed = 0xDEADBEEFL;
  static final int fileSize = 256;
  static final String fileNameBase = "s3file#NUMBER#.dat";
  static final int blockSize = 2048;
  static final byte replication = 1;
  static final int defaultBufferSize = 1024;

  @Test
  public void testConcat() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_NAMENODE_OBJECT_STORAGE_ENABLED_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).checkDataNodeHostConfig(true).build();
    FileSystem fs = cluster.getFileSystem();

    try {
      int nFiles = 3;
      Path[] paths = new Path[nFiles];
      byte[][] filesContent = new byte[nFiles][fileSize];
      Integer fileSuffix = 1;
      for(int i = 0; i < nFiles; i++, fileSuffix++) {
        paths[i] = new Path(fileNameBase.replace("#NUMBER#", fileSuffix.toString()));
        fs.mkdirs(paths[i].getParent());
        filesContent[i] =  AppendTestUtil.randomBytes(seed, fileSize);
        FSDataOutputStream stm = fs.create(paths[i], true, fs.getConf()
                .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, defaultBufferSize), replication, blockSize);
        stm.write(filesContent[i], 0, fileSize);
        stm.close();
        fs.deleteOnExit(paths[i]);
      }

      // Concat all files
      fs.concat(paths[0], new Path[]{paths[1], paths[2]});
      long len = fs.getFileStatus(paths[0]).getLen();
      assertTrue(paths[0] + " should be of size " + fileSize*3 +
              " but found to be of size " + len, len == fileSize*3);
      assertTrue(paths[1] + " should not exist.", !fs.exists(paths[1]));
      assertTrue(paths[2] + " should not exist.", !fs.exists(paths[2]));

      // Verify content
      // From initial file contents
      byte[] first3Content = append(filesContent[0], fileSize, filesContent[1], fileSize);
      first3Content = append(first3Content, fileSize*2, filesContent[2], fileSize);

      // From HDFS
      FSDataInputStream fis = fs.open(paths[0], defaultBufferSize);
      byte[] readContent = new byte[fileSize*3];
      int bytesRead = fis.read(readContent, 0, fileSize*3);
      assertTrue("The number of bytes read is " + bytesRead + " but should be " + fileSize*3,
              bytesRead == fileSize*3);
      assertTrue("Contents from initial source and from HDFS do not match.",
              equalContent(first3Content, 0, fileSize*3, readContent, 0, bytesRead));

      // Verify ranged read across a subset of objects
      // Ranges for each S3 object:
      // [---1###][###2###][###3---]
      bytesRead = fis.read(128, readContent, 0, 512);
      assertTrue("The number of bytes read is " + bytesRead + " but should be " + 512,
              bytesRead == 512);
      assertTrue("Contents from initial source and from HDFS do not match.",
              equalContent(first3Content, 128, 512, readContent, 0, bytesRead));
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
