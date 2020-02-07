package io.hops.transaction.lock;

import io.hops.exception.TransactionContextException;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.transaction.EntityManager;
import io.hops.transaction.context.HdfsTransactionContextMaintenanceCmds;
import org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous;

import java.io.IOException;

class IndividualS3ObjectLock extends BaseIndividualS3ObjectLock {
  private final static long NON_EXISTING_OBJECT = Long.MIN_VALUE;
  protected final long objectId;
  protected final long inodeId;

  public IndividualS3ObjectLock() {
    this.objectId = NON_EXISTING_OBJECT;
    this.inodeId = S3ObjectInfoContiguous.NON_EXISTING_ID;
  }

  IndividualS3ObjectLock(long objectId, INodeIdentifier inode) {
    this.objectId = objectId;
    this.inodeId = inode == null ? S3ObjectInfoContiguous.NON_EXISTING_ID : inode.getInodeId();
  }

  @Override
  protected void acquire(TransactionLocks transactionLocks) throws IOException {
    readObject(objectId, inodeId);
  }

  private void announceObjectDoesNotExist() throws TransactionContextException {
    EntityManager.snapshotMaintenance(HdfsTransactionContextMaintenanceCmds.S3ObjectDoesNotExist, objectId, inodeId);
  }

  protected void announceEmptyFile(long inodeFileId) throws TransactionContextException {
    EntityManager.snapshotMaintenance(HdfsTransactionContextMaintenanceCmds.EmptyFile, inodeFileId);
  }

  protected void readObject(long objectId, long inodeId) throws IOException {
    if(objectId != NON_EXISTING_OBJECT || objectId > 0) {
      S3ObjectInfoContiguous result = acquireLock(DEFAULT_LOCK_TYPE, S3ObjectInfoContiguous.Finder.ByObjectIdAndINodeId,
              objectId, inodeId);
      if(result != null) {
        objects.add(result);
      } else {
        announceObjectDoesNotExist();
      }
    }
  }
}
