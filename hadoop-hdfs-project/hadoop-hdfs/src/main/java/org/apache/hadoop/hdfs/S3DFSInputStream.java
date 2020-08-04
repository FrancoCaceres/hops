package org.apache.hadoop.hdfs;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.RangedS3Object;
import org.apache.hadoop.hdfs.protocol.S3File;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.util.IdentityHashStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.EnumSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_KEY;

@InterfaceAudience.Private
public class S3DFSInputStream extends FSInputStream implements ByteBufferReadable, CanSetReadahead,
        CanSetDropBehind, HasEnhancedByteBufferAccess, CanUnbuffer {
  static final Log LOG = LogFactory.getLog(S3DFSInputStream.class);
  private final DFSClient dfsClient;
  private final String src;
  private final boolean verifyChecksum; // TODO FCG Add checksum type field to object record in DB
  private AtomicBoolean closed;
  private long pos;
  private long remaining;
  private final DFSInputStream.ReadStatistics readStatistics;
  private byte[] oneByteBuf;
  private S3File s3File;
  private FileEncryptionInfo fileEncryptionInfo = null;
  private IdentityHashStore<ByteBuffer, ByteBufferPool> extendedReadBuffers;
  private Downloader downloader;
  private final ListeningExecutorService executorService;
  private FilePart currentPart;
  private static final String AWS_NO_SUCH_KEY_ERROR_CODE = "NoSuchKey";
  private static final String AWS_NO_SUCH_VERSION_ERROR_CODE = "NoSuchVersion";
  private final boolean crc32Enabled;

  public S3DFSInputStream(DFSClient dfsClient, String src, boolean verifyChecksum)
          throws IOException {
    this.dfsClient = dfsClient;
    this.src = src;
    this.verifyChecksum = verifyChecksum;
    this.closed = new AtomicBoolean(false);
    this.pos = 0;
    this.readStatistics = new DFSInputStream.ReadStatistics();
    this.s3File = dfsClient.getS3File(src, 0);
    this.fileEncryptionInfo = s3File.getFileEncryptionInfo();
    if(s3File == null) {
      throw new IOException("Cannot open S3 file " + src);
    }
    this.executorService = MoreExecutors.listeningDecorator(dfsClient.getThreadPoolExecutor());
    this.crc32Enabled = dfsClient.getConfiguration().getBoolean(DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_KEY,
            DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_DEFAULT);
    startDownloader(0L);
  }

  public void logCurrentStatus() {
    LOG.info(String.format("src[%s] pos[%d] remaining[%d] size[%d]", src, pos, remaining, s3File.getFileLength()));
  }

  private void stopDownloader() throws IOException {
    if(downloader == null) {
      return;
    }
    downloader.terminate();
    try {
      downloader.join();
      downloader.cleanup();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void startDownloader(long startPos) throws IOException {
    stopDownloader();
    this.currentPart = null;
    remaining = s3File.getFileLength() - startPos;
    this.downloader = new Downloader(s3File, startPos);
    downloader.start();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      DFSClient.LOG.debug("S3DFSInputStream has been closed already");
      return;
    }
    stopDownloader();
    dfsClient.checkOpen();
    super.close();
  }

  @Override
  public synchronized int read() throws IOException {
    if (oneByteBuf == null) {
      oneByteBuf = new byte[1];
    }
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  @Override
  public synchronized int read(final byte[] buf, int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }
    if(pos >= s3File.getFileLength()) {
      return -1;
    }
    int realLen = (int) Math.min(len, remaining);
    int result = 0;
    while(realLen > 0) {
      if(currentPart == null || currentPart.isDoneReading() ) {
        try {
          if(currentPart != null) {
            currentPart.setContent(null);
          }
          currentPart = downloader.filePartCallables.take().get();
          if(currentPart.hasFailed()) {
            throw currentPart.getEx();
          }
        } catch (Exception e) {
          if(e instanceof AmazonServiceException) {
            AmazonServiceException ase = (AmazonServiceException)e;
            boolean isNoSuchKey = AWS_NO_SUCH_KEY_ERROR_CODE.equals(ase.getErrorCode());
            boolean isNoSuchVersion = AWS_NO_SUCH_VERSION_ERROR_CODE.equals(ase.getErrorCode());
            if(isNoSuchKey || isNoSuchVersion) {
              stopDownloader();
              startDownloader(pos);
            }
          }
          throw new IOException(e);
        }
      }
      int toCopy = Math.min(realLen, currentPart.getRemaining());
      System.arraycopy(currentPart.getContent(), currentPart.getOffset() + currentPart.getRead(), buf, off, toCopy);
      currentPart.advance(toCopy);
      off += toCopy;
      result += toCopy;
      realLen -= toCopy;
      remaining -= toCopy;
      pos += toCopy;

    }
    readStatistics.addRemoteBytes(result);
    return result;
  }

  @Override
  public synchronized int read(final ByteBuffer buf) throws IOException {
    int amountToRead = buf.remaining();
    int initialPosition = buf.position();
    byte buffer[] = new byte[amountToRead];
    int actuallyRead = read(buffer, 0, amountToRead);
    if(actuallyRead > 0) {
      buf.put(buffer);
      buf.position(initialPosition);
    }
    return actuallyRead;
  }

  @Override
  public long skip(long n) throws IOException {
    if ( n > 0 ) {
      long curPos = getPos();
      long fileLen = getFileLength();
      if( n+curPos > fileLen ) {
        n = fileLen - curPos;
      }
      seek(curPos+n);
      return n;
    }
    return n < 0 ? -1 : 0;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new IOException("Cannot seek after EOF");
    }
    if (targetPos < 0) {
      throw new IOException("Cannot seek to negative offset");
    }
    if (closed.get()) {
      throw new IOException("Stream is closed!");
    }
    pos = targetPos;
    startDownloader(pos);
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    return true;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int available() throws IOException {
    if (closed.get()) {
      throw new IOException("Stream closed");
    }
    final long remaining = getFileLength() - pos;
    return remaining <= Integer.MAX_VALUE ? (int)remaining: Integer.MAX_VALUE;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  @Override
  public synchronized void setReadahead(Long readahead) throws IOException, UnsupportedOperationException {
    throw new UnsupportedActionException("setReadahead not supported in S3");
  }

  @Override
  public synchronized void setDropBehind(Boolean dropCache) throws IOException, UnsupportedOperationException {
    throw new UnsupportedActionException("setDropBehind not supported in S3");
  }

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0).asReadOnlyBuffer();

  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool, int maxLength, EnumSet<ReadOption> opts)
      throws IOException, UnsupportedOperationException {
    if(maxLength == 0) {
      return EMPTY_BUFFER;
    } else if (maxLength < 0) {
      throw new IllegalArgumentException("can't read a negative " +
              "number of bytes.");
    }
    ByteBuffer buffer =  ByteBufferUtil.fallbackRead(this, bufferPool, maxLength);
    if(buffer != null) {
      getExtendedReadBuffers().put(buffer, bufferPool);
    }
    return buffer;
  }

  @Override
  public synchronized void releaseBuffer(ByteBuffer buffer) {
    if (buffer == EMPTY_BUFFER) return;
    ByteBufferPool pool = getExtendedReadBuffers().remove(buffer);
    if (pool == null) {
      throw new IllegalArgumentException("tried to release a buffer " +
              "that was not created by this stream, " + buffer);
    }
    pool.putBuffer(buffer);
  }

  private synchronized IdentityHashStore<ByteBuffer, ByteBufferPool> getExtendedReadBuffers() {
    if (extendedReadBuffers == null) {
      extendedReadBuffers = new IdentityHashStore<ByteBuffer, ByteBufferPool>(0);
    }
    return extendedReadBuffers;
  }

  @Override
  public synchronized void unbuffer() {

  }

  private synchronized void checkOpen() throws IOException {
    dfsClient.checkOpen();
    if(closed.get()) {
      throw new ClosedChannelException();
    }
  }

  public long getFileLength() {
    return s3File.getFileLength();
  }

  public DFSInputStream.ReadStatistics getReadStatistics() {
    return new DFSInputStream.ReadStatistics(readStatistics);
  }

  public void clearReadStatistics() {
    readStatistics.clear();
  }

  public synchronized FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  private long getPartSize(RangedS3Object rangedS3Object) {
    GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(rangedS3Object.getBucket(),
            rangedS3Object.getKey(), rangedS3Object.getVersionId()).withPartNumber(1);

    return dfsClient.getS3().getObjectMetadata(getObjectMetadataRequest).getContentLength();
  }

  // TODO FCG: deduplicate eventually
  private long computeCRC32(byte[] content, int contentLength) {
    CRC32 crc32 = new CRC32();
    crc32.update(content, 0, contentLength);
    return crc32.getValue();
  }

  class FilePart {
    private int length;
    private int read;
    private final int offset;
    private byte[] content;
    private Exception ex;

    public FilePart(int length, int read, byte[] content, int offset) {
      this.length = length;
      this.read = read;
      this.content = content;
      this.offset = offset;
    }

    public int getLength() {
      return length;
    }

    public int getRead() {
      return read;
    }

    public byte[] getContent() {
      return content;
    }

    public void setContent(byte[] content) {
      this.content = content;
    }

    public int getRemaining() {
      return length - read;
    }

    public boolean isDoneReading() {
      return read == length;
    }

    public void advance(int n) {
      read += n;
    }

    public boolean hasFailed() {
      return ex != null;
    }

    public Exception getEx() {
      return ex;
    }

    public void setEx(Exception ex) {
      this.ex = ex;
    }

    public int getOffset() {
      return offset;
    }
  }

  class Downloader extends Thread {
    private final S3File s3File;
    private final long startPos;
    private final BlockingQueue<ListenableFuture<FilePart>> filePartCallables;
    private boolean running;

    Downloader(S3File s3File, long startPos) {
      this.s3File = s3File;
      this.startPos = startPos;
      this.filePartCallables = new LinkedBlockingQueue<>();
      this.running = true;
    }

    public void run() {
      for(RangedS3Object rangedS3Object : s3File.getObjectsInRange(startPos, s3File.getFileLength())) {
        long partSize = getPartSize(rangedS3Object);
        if(crc32Enabled) {
          partSize -= Long.BYTES; // do not count CRC32 bytes
        }
        for(RangedS3Object.RangedPart rangedPart :
          rangedS3Object.getAllRangedParts(partSize)) {
          downloadPart(partSize, rangedS3Object, rangedPart);
          if(!running) {
            break;
          }
        }
        if(!running) {
          break;
        }
      }
    }

    void downloadPart(long partSize, RangedS3Object obj, RangedS3Object.RangedPart part) {
      GetObjectRequest req = getS3PartGetObjectRequest(obj, part.getPartNumber());
      ListenableFuture<FilePart> callable =
        executorService.submit(() -> {
          int fullContentLen = (int)partSize;
          if(crc32Enabled) {
            fullContentLen += Long.BYTES; // TODO FCG fix casting (should not be required)
          }

          FilePart filePart = new FilePart(part.getLength(), 0, new byte[fullContentLen], (int)part.getOffset());
          int retries = 3;
          while(retries > 0) {
            try {
              com.amazonaws.services.s3.model.S3Object s3Object = dfsClient.getS3().getObject(req);
              int totalRead = 0;
              int taskLen = fullContentLen;
              int taskOff = 0;
              while(totalRead < fullContentLen) {
                int currentRead = s3Object.getObjectContent().read(filePart.getContent(), taskOff, taskLen);
                if(currentRead == -1) {
                  break;
                }
                taskOff += currentRead;
                taskLen -= currentRead;
                totalRead += currentRead;
              }
              s3Object.getObjectContent().close();
              if(crc32Enabled) {
                byte[] crc32 = new byte[Long.BYTES];
                System.arraycopy(filePart.getContent(), (int)partSize, crc32, 0, Long.BYTES);
                long responseCRC32 = Longs.fromByteArray(crc32);
                // TODO FCG: fix casting
                long computedCRC32Val = computeCRC32(filePart.getContent(), (int)part.getOffset() + part.getLength());
                if(responseCRC32 != computedCRC32Val) {
                  // TODO FCG: improve exception throwing
                  throw new Exception(String.format("Computed S3 part CRC32 [%d] does not match attached CRC32 [%d]",
                          computedCRC32Val, responseCRC32));
                }
              }
              filePart.setEx(null);
              return filePart;
            } catch (Exception ex) {
              filePart.setEx(ex);
              retries--;
            }
          }
          if(filePart.hasFailed()) {
            filePart.setContent(null);
          }
          return filePart;
        });
      filePartCallables.add(callable);
    }

    public void terminate() {
      running = false;
    }

    public void cleanup() {
      Futures.allAsList(filePartCallables).cancel(true);
      filePartCallables.clear();
    }

    private GetObjectRequest getS3PartGetObjectRequest(RangedS3Object obj, int objectPartNumber) {
      return new GetObjectRequest(obj.getBucket(), obj.getKey(), obj.getVersionId())
              .withPartNumber(objectPartNumber);
    }
  }
}
