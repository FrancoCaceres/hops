package org.apache.hadoop.hdfs;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.List;

/*
* TODO Parallelize part-upload requests to conform to S3's
*  performance guidelines:
*  https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance-design-patterns.html
* */
public class S3UploadOutputStream extends OutputStream {
  private int partNumber;
  private final byte[] buf;
  private int count;
  private final List<PartETag> completedParts;
  private final String bucket;
  private final String key;
  private final AmazonS3 s3;
  private final String uploadId;
  private boolean closed;
  private String versionId; // Obtained after completing upload
  private long size; // Incremented with every uploadPart()

  public S3UploadOutputStream(Configuration conf, String src) {
    partNumber = 1;
    final int partSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_OBJECT_STORAGE_PART_SIZE_KEY,
            DFSConfigKeys.DFS_CLIENT_OBJECT_STORAGE_PART_SIZE_DEFAULT);
    buf = new byte[partSize];
    completedParts = new LinkedList<>();
    bucket = conf.get(DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_BUCKET_KEY, null);
    Preconditions.checkNotNull(bucket, "S3 bucket not provided in configuration");
    String bucketRegion = conf.get(DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_BUCKET_REGION_KEY, null);
    Preconditions.checkNotNull(bucketRegion, "S3 bucket region not provided in configuration");
    key = DFSUtil.removeLeadingSlash(src);

    s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(Regions.fromName(bucketRegion))
            .build();
    count = 0;
    size = 0;
    closed = false;
    InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(bucket, key);
    InitiateMultipartUploadResult res = s3.initiateMultipartUpload(req);
    uploadId = res.getUploadId();
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if(isClosed()) {
      throw new ClosedChannelException();
    }
    buf[count++] = (byte)b;
    if(count == buf.length) {
      uploadPart();
    }
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws ClosedChannelException {
    if(isClosed()) {
      throw new ClosedChannelException();
    }
    int n = 0;
    while(n < len) {
      int bytesToFill = buf.length - count;
      int writtenBytes = Math.min(bytesToFill, len);
      System.arraycopy(b, off, buf, count, writtenBytes);
      count += writtenBytes;
      if(count == buf.length) {
        uploadPart();
      }
      n += writtenBytes;
    }
  }

  private synchronized void uploadPart() {
    UploadPartRequest req = new UploadPartRequest()
            .withBucketName(bucket)
            .withKey(key)
            .withUploadId(uploadId)
            .withInputStream(new ByteArrayInputStream(buf, 0, count))
            .withPartNumber(partNumber)
            .withPartSize(count);
    UploadPartResult uploadResult = s3.uploadPart(req);
    completedParts.add(uploadResult.getPartETag());

    if(DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug(
        String.format("S3UploadOutputStream#uploadPart part %d uploaded successfully", partNumber));
    }

    size += count;
    count = 0;
    partNumber++;
  }

  @Override
  public synchronized void close() throws ClosedChannelException {
    throwIfClosed();

    if(count > 0) {
      uploadPart();
    }

    CompleteMultipartUploadRequest req =
            new CompleteMultipartUploadRequest(bucket, key, uploadId, completedParts);
    CompleteMultipartUploadResult res = s3.completeMultipartUpload(req);
    versionId = res.getVersionId();

    if(DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug(
              String.format("S3UploadOutputStream#close file %s uploaded successfully", key));
    }

    closed = true;
  }

  private synchronized boolean isClosed() {
    return closed;
  }

  private synchronized void throwIfClosed() throws ClosedChannelException {
    if(isClosed()) {
      throw new ClosedChannelException();
    }
  }

  private synchronized void throwIfOpen() {
    if(!isClosed()) {
      throw new IllegalStateException("Channel should be closed");
    }
  }

  synchronized long getSize() throws ClosedChannelException {
    throwIfOpen();
    return size;
  }

  synchronized String getVersionId() throws ClosedChannelException {
    throwIfOpen();
    return versionId;
  }
}
