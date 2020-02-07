package io.hops.transaction.lock;

import com.google.common.collect.Iterables;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class S3ObjectLock extends IndividualS3ObjectLock {
  private final List<INodeFile> files;

  S3ObjectLock() {
    super();
    this.files = new ArrayList<>();
  }

  S3ObjectLock(long objectId, INodeIdentifier inode) {
    super(objectId, inode);
    this.files = new ArrayList<>();
  }

  @Override
  protected void acquire(TransactionLocks locks) throws IOException {
    boolean individualObjectAlreadyRead = false;
    if(locks.containsLock(Type.INode)) {
      BaseINodeLock inodeLock = (BaseINodeLock) locks.getLock(Type.INode);
      Iterable objs = Collections.EMPTY_LIST;
      for(INode inode : inodeLock.getAllResolvedINodes()) {

        if(inode instanceof INodeFile) {
          Collection<S3ObjectInfoContiguous> inodeObjects =
                  acquireLockList(DEFAULT_LOCK_TYPE, S3ObjectInfoContiguous.Finder.ByINodeId, inode.getId());

          if(!individualObjectAlreadyRead) {
            individualObjectAlreadyRead = inode.getId() == inodeId;
          }

          objs = Iterables.concat(objs, inodeObjects);
          files.add((INodeFile) inode);
        }
      }
    } else {
      throw new TransactionLocks.LockNotAddedException("S3ObjectBlock must come after an InodeLock");
    }

    if(!individualObjectAlreadyRead) {
      super.acquire(locks);
    }
  }

  Collection<INodeFile> getFiles() {
    return files;
  }
}
