package io.hops.transaction.context;

import com.google.common.collect.Collections2;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.S3ObjectInfoDataAccess;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.transaction.lock.Lock;
import io.hops.transaction.lock.S3ObjectLock;
import io.hops.transaction.lock.SqlBatchedS3ObjectsLock;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous;
import org.apache.hadoop.hdfs.server.namenode.INode;

import java.util.*;

public class S3ObjectInfoContext extends BaseEntityContext<Long, S3ObjectInfoContiguous> {
  private final static int DEFAULT_NUM_OBJECTS_PER_INODE = 1;
  private final Map<Long, List<S3ObjectInfoContiguous>> inodeObjects = new HashMap<>();
  private final S3ObjectInfoDataAccess<S3ObjectInfoContiguous> dataAccess;
  private final List<S3ObjectInfoContiguous> concatRemovedObjs = new ArrayList<>();
  private boolean foundByInode = false;

  public S3ObjectInfoContext(S3ObjectInfoDataAccess<S3ObjectInfoContiguous> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void clear() throws TransactionContextException {
    super.clear();
    inodeObjects.clear();
    concatRemovedObjs.clear();
  }

  @Override
  public void update(S3ObjectInfoContiguous objectInfo) throws TransactionContextException {
    super.update(objectInfo);
    updateInodeObjects(objectInfo);
    if(isLogTraceEnabled()) {
      log("update-s3objectinfo", "oid", objectInfo.getObjectId(), "inodeId", objectInfo.getInodeId(),
              "obj index", objectInfo.getObjectIndex());
    }
  }

  @Override
  public void remove(S3ObjectInfoContiguous objectInfo) throws TransactionContextException {
    super.remove(objectInfo);
    removeObjectFromInodeObjects(objectInfo);
    if(isLogTraceEnabled()) {
      log("removed-s3objectinfo", "oid", objectInfo.getInodeId());
    }
  }

  @Override
  public void prepare(TransactionLocks tlm) throws TransactionContextException, StorageException {
    if(foundByInode && !(tlm.getLock(Lock.Type.S3Object) instanceof S3ObjectLock)
        && !(tlm.getLock(Lock.Type.S3Object) instanceof SqlBatchedS3ObjectsLock)) {
      throw new TransactionContextException("You can't find ByINodeId(s) when taking the lock only on one S3 object");
    }
    Collection<S3ObjectInfoContiguous> removed = new ArrayList<>(getRemoved());
    removed.addAll(concatRemovedObjs);
    dataAccess.prepare(removed, getAdded(), getModified());
  }

  @Override
  public S3ObjectInfoContiguous find(FinderType<S3ObjectInfoContiguous> finder,
     Object... params) throws TransactionContextException, StorageException {
    S3ObjectInfoContiguous.Finder oFinder = (S3ObjectInfoContiguous.Finder) finder;
    switch(oFinder) {
      case ByObjectIdAndINodeId:
        return findById(oFinder, params);
      case ByMaxObjectIndexForINodeId:
        return findMaxObject(oFinder, params);
      case ByINodeIdAndIndex:
        return findByInodeIdAndIndex(oFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<S3ObjectInfoContiguous> findList(FinderType<S3ObjectInfoContiguous> finder,
     Object... params) throws TransactionContextException, StorageException {
    S3ObjectInfoContiguous.Finder oFinder = (S3ObjectInfoContiguous.Finder) finder;
    switch(oFinder) {
      case ByINodeId:
        foundByInode = true;
        return findByInodeId(oFinder, params);
      case ByObjectIdsAndINodeIds:
        return findBatch(oFinder, params);
      case ByINodeIds:
        foundByInode = true;
        return findByInodeIds(oFinder, params);
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  Long getKey(S3ObjectInfoContiguous s3ObjectInfoContiguous) {
    return s3ObjectInfoContiguous.getObjectId();
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
                                  Object... params) throws TransactionContextException {
    HdfsTransactionContextMaintenanceCmds hopCmds = (HdfsTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        //delete the previous row from db
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        break;
      case Concat:
        INodeCandidatePrimaryKey trg_param =
                (INodeCandidatePrimaryKey) params[0];
        List<INodeCandidatePrimaryKey> srcs_param = (List<INodeCandidatePrimaryKey>) params[1];
        List<S3ObjectInfoContiguous> oldObjects = (List<S3ObjectInfoContiguous>) params[3];
        deleteObjectsForConcat(trg_param, srcs_param, oldObjects);
        break;
    }
  }

  private void deleteObjectsForConcat(INodeCandidatePrimaryKey trg_param, List<INodeCandidatePrimaryKey> deleteINodes,
                                      List<S3ObjectInfoContiguous> oldObjects)
          throws TransactionContextException {
    if (!getRemoved().isEmpty()) {//in case of concat new s3object rows are added by the concat fn
      throw new IllegalStateException(
              "Concat file(s) whose s3 objects are changed. During rename and move no s3 objects should have been changed.");
    }

    for (S3ObjectInfoContiguous obj : oldObjects) {
      INodeCandidatePrimaryKey pk = new INodeCandidatePrimaryKey(obj.getInodeId());
      if (deleteINodes.contains(pk)) {
        //remove the object
        concatRemovedObjs.add(obj);
        if(isLogTraceEnabled()) {
          log("snapshot-maintenance-removed-s3objectinfo", "oid", obj.getObjectId(),
                  "inodeId", obj.getInodeId());
        }
      }
    }
  }

  private List<S3ObjectInfoContiguous> findByInodeId(S3ObjectInfoContiguous.Finder oFinder, final Object[] params)
          throws TransactionContextException, StorageException {
    List<S3ObjectInfoContiguous> result = null;
    final Long inodeId = (Long) params[0];
    if(inodeObjects.containsKey(inodeId)) {
      result = inodeObjects.get(inodeId);
      hit(oFinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(oFinder, params);
      result = dataAccess.findByInodeId(inodeId);
      inodeObjects.put(inodeId, syncObjectInfoInstances(result));
      miss(oFinder, result, "inodeid", inodeId);
    }
    return result;
  }

  private List<S3ObjectInfoContiguous> findBatch(S3ObjectInfoContiguous.Finder oFinder, Object[] params)
          throws TransactionContextException, StorageException {
    List<S3ObjectInfoContiguous> result = null;
    final long[] objectIds = (long[]) params[0];
    final long[] inodeIds = (long[]) params[1];
    aboutToAccessStorage(oFinder, params);
    result = dataAccess.findByIds(objectIds, inodeIds);
    miss(oFinder, result, "ObjectIds", Arrays.toString(objectIds), "InodeIds", Arrays.toString(inodeIds));
    return syncObjectInfoInstances(result, objectIds);
  }

  private List<S3ObjectInfoContiguous> findByInodeIds(S3ObjectInfoContiguous.Finder oFinder, Object[] params)
          throws TransactionContextException, StorageException {
    List<S3ObjectInfoContiguous> result = null;
    final long[] ids = (long[]) params[0];
    aboutToAccessStorage(oFinder, params);
    result = dataAccess.findByInodeIds(ids);
    for(long id: ids) {
      inodeObjects.put(id, null);
    }
    miss(oFinder, result, "InodeIds", Arrays.toString(ids));
    return syncObjectInfoInstances(result, true);
  }

  private S3ObjectInfoContiguous findByInodeIdAndIndex(S3ObjectInfoContiguous.Finder oFinder, final Object[] params)
          throws TransactionContextException {
    List<S3ObjectInfoContiguous> objects = null;
    S3ObjectInfoContiguous result = null;
    final Long inodeId = (Long) params[0];
    final Integer index = (Integer) params[1];
    if(inodeObjects.containsKey(inodeId)) {
      objects = inodeObjects.get(inodeId);
      for(S3ObjectInfoContiguous obj : objects) {
        if(obj.getObjectIndex() == index) {
          result = obj;
          break;
        }
      }
      hit(oFinder, result, "inodeid", inodeId);
    } else {
      throw new TransactionContextException("this function can't be called without owning a lock on the s3object");
    }
    return result;
  }

  private S3ObjectInfoContiguous findById(S3ObjectInfoContiguous.Finder oFinder, final Object[] params)
          throws TransactionContextException, StorageException {
    S3ObjectInfoContiguous result = null;
    long objectId = (Long) params[0];
    Long inodeId = null;
    if(params.length > 1 && params[1] != null) {
      inodeId = (Long) params[1];
    }
    if(contains(objectId)) {
      result = get(objectId);
      hit(oFinder, result, "oid", objectId, "inodeId", inodeId != null ? Long.toString(inodeId) : "NULL");
    } else {
      if(inodeId == null) {
        throw new IllegalArgumentException(
                Thread.currentThread().getId() + " InodeId is not set for s3object " + objectId);
      }
      aboutToAccessStorage(oFinder, params);
      result = dataAccess.findById(objectId, inodeId);
      gotFromDB(objectId, result);
      updateInodeObjects(result);
      miss(oFinder, result, "oid", objectId, "inodeId", inodeId);
    }
    return result;
  }

  private S3ObjectInfoContiguous findMaxObject(S3ObjectInfoContiguous.Finder oFinder, final Object[] params) {
    final long inodeId = (Long) params[0];
    Collection<S3ObjectInfoContiguous> notRemovedObjs = Collections2.filter(filterValuesNotOnState(State.REMOVED),
            (input) -> input.getInodeId() == inodeId);
    if(notRemovedObjs.size() > 0) {
      S3ObjectInfoContiguous result = Collections.max(notRemovedObjs, S3ObjectInfoContiguous.Order.ByS3ObjectIndex);
      hit(oFinder, result, "inodeId", inodeId);
      return result;
    } else {
      miss(oFinder, (S3ObjectInfoContiguous) null, "inodeId", inodeId);
      return null;
    }
  }

  private List<S3ObjectInfoContiguous> syncObjectInfoInstances(List<S3ObjectInfoContiguous> newObjects,
     long[] objectIds) {
    List<S3ObjectInfoContiguous> result = syncObjectInfoInstances(newObjects);
    for(long objectId : objectIds) {
      if(!contains(objectId)) {
        gotFromDB(objectId, null);
      }
    }
    return result;
  }

  private List<S3ObjectInfoContiguous> syncObjectInfoInstances(List<S3ObjectInfoContiguous> newObjects) {
    return syncObjectInfoInstances(newObjects, false);
  }

  private List<S3ObjectInfoContiguous> syncObjectInfoInstances(List<S3ObjectInfoContiguous> newObjects,
      boolean syncInodeObjects) {
    List<S3ObjectInfoContiguous> finalList = new ArrayList<>();

    for(S3ObjectInfoContiguous object : newObjects) {
      if(isRemoved(object.getObjectId())) {
        continue;
      }

      gotFromDB(object);
      finalList.add(object);

      if(syncInodeObjects) {
        List<S3ObjectInfoContiguous> objectList = inodeObjects.get(object.getInodeId());
        if(objectList == null) {
          objectList = new ArrayList<>();
          inodeObjects.put(object.getInodeId(), objectList);
        }
        objectList.add(object);
      }
    }

    return finalList;
  }

  private void updateInodeObjects(S3ObjectInfoContiguous newObject) {
    if(newObject == null) {
      return;
    }

    List<S3ObjectInfoContiguous> objectList = inodeObjects.get(newObject.getInodeId());

    if(objectList != null) {
       int idx = objectList.indexOf(newObject);
       if(idx != -1) {
         objectList.set(idx, newObject);
       } else {
         objectList.add(newObject);
       }
    } else {
      List<S3ObjectInfoContiguous> list = new ArrayList<>(DEFAULT_NUM_OBJECTS_PER_INODE);
      list.add(newObject);
      inodeObjects.put(newObject.getInodeId(), list);
    }
  }

  private void removeObjectFromInodeObjects(S3ObjectInfoContiguous object) throws TransactionContextException {
    List<S3ObjectInfoContiguous> objectList = inodeObjects.get(object.getInodeId());
    if(objectList != null) {
      if(!objectList.remove(object)) {
        throw new TransactionContextException("Trying to remove an s3object that does not exist");
      }
    }
  }
}
