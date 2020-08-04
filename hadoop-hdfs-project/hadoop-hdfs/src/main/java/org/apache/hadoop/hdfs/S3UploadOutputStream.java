package org.apache.hadoop.hdfs;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.BinaryUtils;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_KEY;

public class S3UploadOutputStream extends OutputStream {
  static final Log LOG = LogFactory.getLog(S3UploadOutputStream.class);
  private int partNumber;
  private byte[] buf;
  private final int partSize;
  private int count;
  private final List<ListenableFuture<PartETag>> partETagsFutures;
  private final String bucket;
  private final String key;
  private final AmazonS3 s3;
  private final String uploadId;
  private boolean closed;
  private String versionId = ""; // Obtained after completing upload
  private long size; // Incremented with every uploadPart()
  private ListeningExecutorService executorService;
  private final boolean crc32Enabled;

  public S3UploadOutputStream(AmazonS3 s3, Configuration conf, String key, ExecutorService threadPoolExecutor) {
    partNumber = 1;
    partSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_OBJECT_STORAGE_PART_SIZE_KEY,
            DFSConfigKeys.DFS_CLIENT_OBJECT_STORAGE_PART_SIZE_DEFAULT);
    buf = new byte[partSize + Long.BYTES]; // part size + space for CRC32 (32 bits)
    partETagsFutures = new LinkedList<>();
    bucket = conf.get(DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_BUCKET_KEY, null);
    Preconditions.checkNotNull(bucket, "S3 bucket not provided in configuration");
    String bucketRegion = conf.get(DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_BUCKET_REGION_KEY, null);
    Preconditions.checkNotNull(bucketRegion, "S3 bucket region not provided in configuration");
    this.key = DFSUtil.removeLeadingSlash(key);

    this.s3 = s3;
    count = 0;
    size = 0;
    closed = false;
    InitiateMultipartUploadRequest req = new InitiateMultipartUploadRequest(bucket, this.key);
    InitiateMultipartUploadResult res = s3.initiateMultipartUpload(req);
    uploadId = res.getUploadId();
    executorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    this.crc32Enabled = conf.getBoolean(DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_KEY,
            DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_DEFAULT);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if(isClosed()) {
      throw new ClosedChannelException();
    }
    buf[count++] = (byte)b;
    if(count == partSize) {
      uploadPart();
    }
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws ClosedChannelException {
    if(isClosed()) {
      throw new ClosedChannelException();
    }
    while(len > 0) {
      int bytesToFill = partSize - count;
      int writtenBytes = Math.min(bytesToFill, len);
      System.arraycopy(b, off, buf, count, writtenBytes);
      count += writtenBytes;
      if(count == partSize) {
        uploadPart();
      }
      off += writtenBytes;
      len -= writtenBytes;
    }
  }

  private void uploadPart() {
    byte[] contentReq = buf.clone();
    int countReq = count;
    int partNumberReq = partNumber;

    ListenableFuture<PartETag> partETagFuture =
      executorService.submit(() -> {
        int fullContentLen = countReq;
        if(crc32Enabled) {
          fullContentLen = partSize + Long.BYTES;
          byte[] crc32 = Longs.toByteArray(computeCRC32(contentReq, countReq));
          System.arraycopy(crc32, 0, contentReq, partSize, Long.BYTES);
        }
        String md5Base64 = computedMd5(contentReq, fullContentLen);
        UploadPartRequest req = new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(key)
                .withUploadId(uploadId)
                .withInputStream(new ByteArrayInputStream(contentReq, 0, fullContentLen))
                .withPartNumber(partNumberReq)
                .withMD5Digest(md5Base64)
                .withPartSize(fullContentLen);
        int retries = 3;
        while(retries > 0) {
          try {
            UploadPartResult uploadResult = s3.uploadPart(req);
            return uploadResult.getPartETag();
          } catch (Exception ex) {
            retries--;
          }
        }
        return null;
      });
    partETagsFutures.add(partETagFuture);

    size += count;
    count = 0;
    partNumber++;
  }

  private String computedMd5(byte[] content, int contentLength) {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(content, 0, contentLength);
      return BinaryUtils.toBase64(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new SdkClientException(e);
    }
  }

  // TODO FCG: deduplicate eventually
  private long computeCRC32(byte[] content, int contentLength) {
    CRC32 crc32 = new CRC32();
    crc32.update(content, 0, contentLength);
    return crc32.getValue();
  }

  @Override
  public synchronized void close() throws IOException {
    throwIfClosed();

    if(count > 0) {
      uploadPart();
    }

    if(size == 0) {
      closed = true;
      return;
    }

    CompleteMultipartUploadRequest req = null;
    List<PartETag> partETags = null;
    // TODO FCG: improve exception handling
    try {
      partETags = Futures.allAsList(partETagsFutures).get();
      for(PartETag partETag : partETags) {
        if(partETag == null) {
          throw new RuntimeException("Failed to upload at least one part to S3");
        }
      }
      req = new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted partUpload: {}", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(String.format("Failed to complete S3 multipart upload for %s, message = %s, " +
                      "partETagsFutures.size = %d, size = %d", key,
              e.getMessage(), partETagsFutures.size(), size), e);
    } catch (ExecutionException e) {
      for(ListenableFuture<PartETag> future : partETagsFutures) {
        future.cancel(true);
      }
      // TODO FCG: abort s3 upload
      throw new RuntimeException(String.format("Failed to complete S3 multipart upload for %s, message = %s, " +
                      "partETagsFutures.size = %d, size = %d", key,
              e.getMessage(), partETagsFutures.size(), size), e);
    }
    try {
      CompleteMultipartUploadResult res = s3.completeMultipartUpload(req);
      versionId = res.getVersionId();
    } catch (Exception ex) {
      String errorMsg = String.format("Failed to complete S3 multipart upload. bucket[%s] key [%s] uploadId[%s] " +
              "partETags[%s]", bucket, key, uploadId, partETags.stream().map(x -> String.format("{%d,%s}",
              x.getPartNumber(),x.getETag())).collect(Collectors.toList()));
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }

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

  public synchronized String getVersionId() throws ClosedChannelException {
    throwIfOpen();
    return versionId;
  }
}
