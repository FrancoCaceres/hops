package io.hops.transaction.lock;

import org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous;

import java.io.IOException;

public class SqlBatchedS3ObjectsLock extends BaseIndividualS3ObjectLock {
  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    Lock inodeLock = locks.getLock(Type.INode);
    if(inodeLock instanceof BatchedINodeLock) {
      long[] inodeIds = ((BatchedINodeLock) inodeLock).getINodeIds();
      objects.addAll(acquireLockList(DEFAULT_LOCK_TYPE, S3ObjectInfoContiguous.Finder.ByINodeIds, inodeIds));
    } else {
      throw new TransactionLocks.LockNotAddedException("HopsBatchedINodeLock wasn't added");
    }
  }
}
