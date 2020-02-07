package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import io.hops.exception.OutOfDBExtentsException;
import io.hops.metadata.hdfs.entity.EncodingPolicy;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.namenode.RetryStartFileException;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.EnumSet;
import java.util.zip.CheckedOutputStream;

@InterfaceAudience.Private
public class S3DFSOutputStream extends DFSOutputStream {
  private static final short REPLICATION = 1;
  private final S3UploadOutputStream s3UploadOutputStream;
  private final CheckedOutputStream checkedOutputStream;

  private S3DFSOutputStream(DFSClient dfsClient, String src, Progressable progress,
                            HdfsFileStatus stat, DataChecksum checksum) throws IOException {
    super(dfsClient, src, progress, stat, checksum);
    this.s3UploadOutputStream = new S3UploadOutputStream(dfsClient.getConfiguration(), src);
    this.checkedOutputStream = new CheckedOutputStream(s3UploadOutputStream, checksum);
  }

  private S3DFSOutputStream(DFSClient dfsClient, String src, HdfsFileStatus stat,
    EnumSet<CreateFlag> flag, Progressable progress, DataChecksum checksum,
    EncodingPolicy policy, final int dbFileMaxSize, boolean forceClientToWriteSFToDisk) throws IOException {
    this(dfsClient, src, progress, stat, checksum);
  }

  static S3DFSOutputStream newStreamForCreate(DFSClient dfsClient, String src,
    FsPermission masked, EnumSet<CreateFlag> flag, boolean createParent,
    long blockSize, Progressable progress, DataChecksum checksum, EncodingPolicy policy,
    final int dbFileMaxSize, boolean forceClientToWriteSFToDisk) throws IOException {
    TraceScope scope =
            dfsClient.newPathTraceScope("newStreamForCreate", src);
    try{
      HdfsFileStatus stat = null;

      // Retry the create if we get a RetryStartFileException up to a maximum
      // number of times
      boolean shouldRetry = true;
      int retryCount = CREATE_RETRY_COUNT;
      while (shouldRetry) {
        shouldRetry = false;
        try {
          stat = dfsClient.namenode.create(src, masked, dfsClient.clientName,
                  new EnumSetWritable<>(flag), createParent, REPLICATION,
                  blockSize, SUPPORTED_CRYPTO_VERSIONS, policy);
          break;
        } catch (RemoteException re) {
          IOException e = re.unwrapRemoteException(
                  AccessControlException.class,
                  DSQuotaExceededException.class,
                  QuotaByStorageTypeExceededException.class,
                  FileAlreadyExistsException.class,
                  FileNotFoundException.class,
                  ParentNotDirectoryException.class,
                  NSQuotaExceededException.class,
                  RetryStartFileException.class,
                  SafeModeException.class,
                  UnresolvedPathException.class,
                  UnknownCryptoProtocolVersionException.class);
          if (e instanceof RetryStartFileException) {
            if (retryCount > 0) {
              shouldRetry = true;
              retryCount--;
            } else {
              throw new IOException("Too many retries because of encryption" + " zone operations", e);
            }
          } else {
            throw e;
          }
        }
      }
      Preconditions.checkNotNull(stat, "HdfsFileStatus should not be null!");
      return new S3DFSOutputStream(dfsClient, src, stat,
              flag, progress, checksum, policy, dbFileMaxSize, forceClientToWriteSFToDisk);
    } finally {
      scope.close();
    }
  }

  @Override
  public void flush() {
    DFSClient.LOG.warn("flush called in S3DFSOutputStream with no effect.");
  }

  @Override
  public void hflush() {
    DFSClient.LOG.warn("hflush called in S3DFSOutputStream with no effect.");
  }

  @Override
  public void hsync() {
    DFSClient.LOG.warn("hsync called in S3DFSOutputStream with no effect.");
  }

  @Override
  public synchronized void write(int b) throws IOException {
    checkClosed();
    this.checkedOutputStream.write(b);
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    checkClosed();
    this.checkedOutputStream.write(b, off, len);
  }

  @Override
  public synchronized void close() throws IOException {
    TraceScope scope =
            dfsClient.newPathTraceScope("S3DFSOutputStream#close", src);
    try {
      closeImpl();
    } finally {
      scope.close();
    }
  }

  protected synchronized void closeImpl() throws IOException {
    checkClosed();
    try {
      closeInternal();
    } catch (ClosedChannelException e) {

    } finally {
      setClosed();
    }
  }

  private void closeInternal() throws IOException {
    TraceScope scope = dfsClient.getTracer().newScope("completeFile");
    try {
      checkedOutputStream.close(); // TODO FCG Get checksum and upload to S3
      completeFile();
    } finally {
      scope.close();
    }
    dfsClient.endFileLease(fileId);
  }

  private void completeFile() throws IOException {
    long localstart = Time.monotonicNow();
    final DfsClientConf conf = dfsClient.getConf();
    long sleeptime = conf.getBlockWriteLocateFollowingInitialDelayMs();
    boolean fileComplete = false;
    int retries = conf.getNumBlockWriteLocateFollowingRetry();

    while (!fileComplete) {
      fileComplete = completeFileInternal();
      if (!fileComplete) {
        final int hdfsTimeout = conf.getHdfsTimeout();
        if (!dfsClient.clientRunning
                || (hdfsTimeout > 0
                && localstart + hdfsTimeout < Time.monotonicNow())) {
          String msg = "Unable to close file because dfsclient " +
                  " was unable to contact the HDFS servers." +
                  " clientRunning " + dfsClient.clientRunning +
                  " hdfsTimeout " + hdfsTimeout;
          DFSClient.LOG.info(msg);
          throw new IOException(msg);
        }
        try {
          if (retries == 0) {
            throw new IOException("Unable to close file because the last block"
                    + " does not have enough number of replicas.");
          }
          retries--;
          Thread.sleep(sleeptime);
          sleeptime *= 2;
          if (Time.monotonicNow() - localstart > 5000) {
            DFSClient.LOG.info("Could not complete " + src + " retrying...");
          }
        } catch (InterruptedException ie) {
          DFSClient.LOG.warn("Caught exception ", ie);
        }
      }
    }
  }

  private boolean completeFileInternal() throws IOException {
    boolean fileComplete;
    byte data[] = null;
    long checksum = checkedOutputStream.getChecksum().getValue();
    // TODO FCG Check from s3UploadOutputStream if data can be stored in DB

    try {
      long size = s3UploadOutputStream.getSize();
      String versionId = s3UploadOutputStream.getVersionId();
      fileComplete =
              dfsClient.namenode.completeS3(src, dfsClient.clientName, versionId, size, checksum, fileId, data);
    } catch (RemoteException e) {
      IOException nonRetryableExceptions =
              e.unwrapRemoteException(NSQuotaExceededException.class,
                      DSQuotaExceededException.class,
                      OutOfDBExtentsException.class );
      if (nonRetryableExceptions != e) {
        throw nonRetryableExceptions; // no need to retry these exceptions
      } else {
        throw e;
      }
    }
    return fileComplete;
  }

  @Override
  protected void checkClosed() throws IOException {
    if(isClosed()) {
      throw new ClosedChannelException();
    }
  }

  boolean isClosed() {
    return closed;
  }

  void setClosed() {
    closed = true;
  }
}
