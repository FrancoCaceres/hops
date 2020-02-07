package io.hops.transaction.lock;

import io.hops.metadata.hdfs.dal.S3ObjectChecksumDataAccess;
import io.hops.metadata.hdfs.entity.S3ObjectChecksum;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.io.IOException;

public class S3ObjectChecksumLock extends Lock {
  private final String target;
  private final int objectIndex;

  S3ObjectChecksumLock(String target, int objectIndex) {
    this.target = target;
    this.objectIndex = objectIndex;
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    BaseINodeLock iNodeLock = (BaseINodeLock) locks.getLock(Type.INode);
    INode iNode = iNodeLock.getTargetINode(target);
    if(BaseINodeLock.isStoredInS3(iNode)) {
      S3ObjectChecksumDataAccess.KeyTuple key =
        new S3ObjectChecksumDataAccess.KeyTuple(iNode.getId(), objectIndex);
      acquireLock(DEFAULT_LOCK_TYPE, S3ObjectChecksum.Finder.ByKeyTuple, key);
    } else {
      LOG.debug("Stuffed Inode: S3ObjectChecksumLock. Skipping acquiring locks on the inode named: " +
              iNode.getLocalName() + " as the file is not stored in S3");
    }
  }

  @Override
  protected Type getType() {
    return Type.BlockChecksum;
  }
}
