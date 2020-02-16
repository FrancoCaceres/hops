package org.apache.hadoop.hdfs;

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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@InterfaceAudience.Private
public class S3DFSInputStream extends FSInputStream implements ByteBufferReadable, CanSetReadahead,
        CanSetDropBehind, HasEnhancedByteBufferAccess, CanUnbuffer {
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

  public S3DFSInputStream(DFSClient dfsClient, String src, boolean verifyChecksum)
          throws IOException {
    this.dfsClient = dfsClient;
    this.src = src;
    this.verifyChecksum = verifyChecksum;
    this.closed = new AtomicBoolean(false);
    this.pos = 0;
    this.readStatistics = new DFSInputStream.ReadStatistics();
    openInfo();
  }

  private void openInfo() throws IOException {
    s3File = dfsClient.getS3File(src, 0);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("s3File = " + s3File);
    }
    if(s3File == null) {
      throw new IOException("Cannot open S3 file " + src);
    }
    fileEncryptionInfo = s3File.getFileEncryptionInfo();
    remaining = s3File.getFileLength();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      DFSClient.LOG.debug("S3DFSInputStream has been closed already");
      return;
    }
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
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }
    if(pos >= s3File.getFileLength()) {
      return -1;
    }
    int realLen = (int) Math.min(len, remaining);
    int result = 0;
    List<RangedS3Object> objectsInRange = s3File.getObjectsInRange(pos, pos + realLen - 1);
    for(RangedS3Object rangedObj : objectsInRange) {
      S3DownloadInputStream s3dwn = new S3DownloadInputStream(rangedObj);
      int currentResult = s3dwn.read(buf, off, realLen);
      result += currentResult;
      off += currentResult;
      pos += currentResult;
      remaining -= currentResult;
      realLen -= currentResult;
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
  public synchronized int read(long position, byte[] buf, int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }
    if(position < 0 || position >= s3File.getFileLength()) {
      return -1;
    }
    int realLen = len;
    if((position + len) > s3File.getFileLength()) {
      realLen = (int)(s3File.getFileLength() - position);
    }

    int result = 0;
    List<RangedS3Object> objectsInRange = s3File.getObjectsInRange(position, position + realLen - 1);
    for(RangedS3Object rangedObj : objectsInRange) {
      S3DownloadInputStream s3dwn = new S3DownloadInputStream(rangedObj);
      int currentResult = s3dwn.read(buf, off, realLen);
      result += currentResult;
      off += currentResult;
      realLen -= currentResult;
    }
    readStatistics.addRemoteBytes(result);
    return result;
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
    remaining = s3File.getFileLength() - pos;
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
}
