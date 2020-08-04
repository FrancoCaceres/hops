package org.apache.hadoop.hdfs;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.RangedS3Object;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class S3DownloadInputStream extends InputStream {
  static final Log LOG = LogFactory.getLog(S3DownloadInputStream.class);
  private final RangedS3Object obj;
  private final AmazonS3 s3;
  private long s3ObjOff;
  private boolean closed;
  private byte[] oneByteBuf;
  private byte[] helperBuffer;
  private final long partSize;
  private ListeningExecutorService executorService;
  private List<ListenableFuture<Object>> downloadCalls;

  public S3DownloadInputStream(AmazonS3 s3, Configuration conf, ExecutorService threadPoolExecutor, RangedS3Object ranged) {
    this.obj = ranged;
    this.s3 = s3;
    this.s3ObjOff = ranged.getOffset();
    this.closed = false;
    this.oneByteBuf = null;
    this.executorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    downloadCalls = new ArrayList<>();
    partSize = getPartSize(1);
    helperBuffer = new byte[2000000];
  }

  private long getPartSize(int number) {
    GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(obj.getBucket(),
            obj.getKey(), obj.getVersionId()).withPartNumber(number);

    return s3.getObjectMetadata(getObjectMetadataRequest).getContentLength();
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
    if(s3ObjOff > obj.getLastIndex()) {
      return -1;
    }

    List<RangedS3Object.RangedPart> parts = obj.getRangedParts(s3ObjOff, len, partSize);
    AtomicInteger res = new AtomicInteger(0);
    for(RangedS3Object.RangedPart part : parts) {
      downloadPart(buf, off, part, res);
      off += part.getLength();
    }

    try {
      Futures.allAsList(downloadCalls).get();
    } catch (AmazonServiceException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      // TODO FCG: handle exceptions
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    if(res.get() >= 0) {
      s3ObjOff += res.get();
      return res.get();
    } else {
      throw new IOException("Unexpected EOS from the reader");
    }
  }

  void downloadPart(byte[] buf, int off, RangedS3Object.RangedPart part, AtomicInteger res) {
    GetObjectRequest req = getS3PartGetObjectRequest(part.getPartNumber());
    ListenableFuture<Object> downloadCall =
      executorService.submit(() -> {
        com.amazonaws.services.s3.model.S3Object s3Object = s3.getObject(req);
        s3Object.getObjectContent().skip(part.getOffset());
        int totalRead = 0;
        int taskLen = part.getLength();
        int taskOff = off;
        while(totalRead < part.getLength()) {
          int currentRead = s3Object.getObjectContent().read(buf, taskOff, taskLen);
          if(currentRead == -1) {
            break;
          }
          taskOff += currentRead;
          taskLen -= currentRead;
          totalRead += currentRead;
        }
        // s3 complains about closing stream without reading the whole part
        while(-1 != s3Object.getObjectContent().read(helperBuffer));
        s3Object.getObjectContent().close();
        res.addAndGet(totalRead);
        return null;
      });
    downloadCalls.add(downloadCall);
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
  }

  private GetObjectRequest getS3GetObjectRequest(long len) {
    return new GetObjectRequest(obj.getBucket(), obj.getKey(), obj.getVersionId())
            .withRange(s3ObjOff, s3ObjOff + len - 1);
  }

  private GetObjectRequest getS3GetObjectRequest(long off, long len) {
    return new GetObjectRequest(obj.getBucket(), obj.getKey(), obj.getVersionId())
            .withRange(s3ObjOff + off, s3ObjOff + off + len - 1);
  }

  private GetObjectRequest getS3PartGetObjectRequest(int partNumber) {
    return new GetObjectRequest(obj.getBucket(), obj.getKey(), obj.getVersionId())
            .withPartNumber(partNumber);
  }
}
