package org.apache.hadoop.hdfs;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.apache.hadoop.hdfs.protocol.RangedS3Object;

import java.io.IOException;
import java.io.InputStream;

public class S3DownloadInputStream extends InputStream {
  private final RangedS3Object obj;
  private final AmazonS3 s3;
  private long pos;
  private long remaining;
  private boolean closed;
  private byte[] oneByteBuf;

  public S3DownloadInputStream(RangedS3Object ranged) {
    this.obj = ranged;
    this.s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(Regions.fromName(ranged.getRegion()))
            .build();
    this.pos = ranged.getOffset();
    this.remaining = ranged.getLength();
    this.closed = false;
    this.oneByteBuf = null;
  }

  @Override
  public int read() throws IOException {
    if (oneByteBuf == null) {
      oneByteBuf = new byte[1];
    }
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  @Override
  public int read(byte[] buf, int off, int len) throws IOException {
    if(closed) {
      throw new IOException("Stream closed");
    }
    if(pos > obj.getLastIndex()) {
      return -1;
    }
    int realLen = (int) Math.min(len, remaining);
    GetObjectRequest req = getS3GetObjectRequest(realLen);
    int result;
    try {
      com.amazonaws.services.s3.model.S3Object s3Object = s3.getObject(req);
      result = s3Object.getObjectContent().read(buf, off, realLen);
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    }
    if(result >= 0) {
      pos += result;
      remaining -= result;
      return result;
    } else {
      throw new IOException("Unexpected EOS from the reader");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
  }

  private GetObjectRequest getS3GetObjectRequest(int len) {
    return new GetObjectRequest(obj.getBucket(), obj.getKey(), obj.getVersionId())
            .withRange(pos, pos + len - 1);
  }
}
