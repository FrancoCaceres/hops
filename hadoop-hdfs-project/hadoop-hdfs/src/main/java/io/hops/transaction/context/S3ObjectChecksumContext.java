package io.hops.transaction.context;

import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.S3ObjectChecksumDataAccess;
import io.hops.metadata.hdfs.entity.S3ObjectChecksum;
import io.hops.transaction.lock.TransactionLocks;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class S3ObjectChecksumContext
  extends BaseEntityContext<S3ObjectChecksumDataAccess.KeyTuple, S3ObjectChecksum> {
  private final S3ObjectChecksumDataAccess<S3ObjectChecksum> dataAccess;
  private final Map<Integer, Collection<S3ObjectChecksum>> inodeToObjectChecksums = new HashMap<>();

  public S3ObjectChecksumContext(S3ObjectChecksumDataAccess<S3ObjectChecksum> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(S3ObjectChecksum objectChecksum) throws TransactionContextException {
    super.update(objectChecksum);
  }

  @Override
  public void remove(S3ObjectChecksum objectChecksum) throws TransactionContextException {
    super.remove(objectChecksum);
  }

  @Override
  public S3ObjectChecksum find(FinderType<S3ObjectChecksum> finder, Object... params)
          throws TransactionContextException, StorageException {
    S3ObjectChecksum.Finder cFinder = (S3ObjectChecksum.Finder) finder;
    switch(cFinder) {
      case ByKeyTuple:
        return findByKeyTuple(cFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodeToObjectChecksums.clear();
  }

  @Override
  public Collection<S3ObjectChecksum> findList(FinderType<S3ObjectChecksum> finder, Object... params)
          throws TransactionContextException, StorageException {
    S3ObjectChecksum.Finder cFinder = (S3ObjectChecksum.Finder) finder;
    switch(cFinder) {
      case ByInodeId:
        return findByINodeId(cFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  S3ObjectChecksumDataAccess.KeyTuple getKey(S3ObjectChecksum objectChecksum) {
    return new S3ObjectChecksumDataAccess.KeyTuple(objectChecksum.getInodeId(), objectChecksum.getObjectIndex());
  }

  @Override
  public void prepare(TransactionLocks tlm) throws TransactionContextException, StorageException {
    for(S3ObjectChecksum objectChecksum : getAdded()) {
      dataAccess.add(objectChecksum);
    }
    for(S3ObjectChecksum objectChecksum : getModified()) {
      dataAccess.update(objectChecksum);
    }
    for(S3ObjectChecksum objectChecksum : getRemoved()) {
      dataAccess.delete(objectChecksum);
    }
  }

  private S3ObjectChecksum findByKeyTuple(S3ObjectChecksum.Finder cFinder, Object[] params)
          throws StorageCallPreventedException, StorageException {
    final S3ObjectChecksumDataAccess.KeyTuple key = (S3ObjectChecksumDataAccess.KeyTuple) params[0];
    if(key == null) {
      return null;
    }
    S3ObjectChecksum result = null;
    if(contains(key)) {
      result = get(key);
      hit(cFinder, result, "KeyTuple", key);
    } else {
      aboutToAccessStorage(cFinder, params);
      result = dataAccess.find(key.getInodeId(), key.getObjectIndex());
      gotFromDB(key, result);
      miss(cFinder, result, "KeyTuple", key);
    }
    return result;
  }

  private Collection<S3ObjectChecksum> findByINodeId(S3ObjectChecksum.Finder cFinder, Object[] params)
          throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    Collection<S3ObjectChecksum> result = null;
    if(inodeToObjectChecksums.containsKey(inodeId)) {
       result = inodeToObjectChecksums.get(inodeId);
       hit(cFinder, result, "inodeId", inodeId);
    } else {
      aboutToAccessStorage(cFinder, params);
      result = dataAccess.findAll((Integer) params[0]);
      gotFromDB(result);
      inodeToObjectChecksums.put(inodeId, result);
      miss(cFinder, result, "inodeId", inodeId);
    }
    return result;
  }
}
