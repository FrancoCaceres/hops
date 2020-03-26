package org.apache.hadoop.hdfs.server.cloud;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.S3Object;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.util.Comparator;

/**
 * S3ObjectInfoContiguous class maintains for a given
 * S3 object the {@link S3ObjectCollection} it is part of
 */
@InterfaceAudience.Private
public class S3ObjectInfoContiguous extends S3Object {
  public static final S3ObjectInfoContiguous[] EMPTY_ARRAY = {};

  public enum Finder implements FinderType<S3ObjectInfoContiguous> {
    ByObjectIdAndINodeId,
    ByINodeId,
    ByINodeIds,
    ByMaxObjectIndexForINodeId,
    ByObjectIdsAndINodeIds,
    ByINodeIdAndIndex;

    @Override
    public Class getType() {
      return S3ObjectInfoContiguous.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByObjectIdAndINodeId:
          return Annotation.PrimaryKey;
        case ByObjectIdsAndINodeIds:
          return Annotation.Batched;
        case ByMaxObjectIndexForINodeId:
          return Annotation.PrunedIndexScan;
        case ByINodeId:
        case ByINodeIdAndIndex:
          return Annotation.PrunedIndexScan;
        case ByINodeIds:
          return Annotation.BatchedPrunedIndexScan;
        default:
          throw new IllegalStateException();
      }
    }
  }

  public enum Order implements Comparator<S3ObjectInfoContiguous> {
    ByS3ObjectIndex() {
      @Override
      public int compare(S3ObjectInfoContiguous o1, S3ObjectInfoContiguous o2) {
        if(o1.getObjectIndex() == o2.getObjectIndex()) {
          throw new IllegalStateException("A file cannot have 2 S3 objects with the same index. index = " +
                  o1.getObjectIndex() + " object1_id = " + o1.getObjectId() + " object2_id = " + o2.getObjectId());
        }
        if(o1.getObjectIndex() < o2.getObjectIndex()) {
          return -1;
        } else {
          return 1;
        }
      }
    },
    ByObjectId() {
      @Override
      public int compare(S3ObjectInfoContiguous o1, S3ObjectInfoContiguous o2) {
        if(o1.getObjectId() == o2.getObjectId()) {
          return 0;
        }
        if(o1.getObjectId() < o2.getObjectId()) {
          return -1;
        } else {
          return 1;
        }
      }
    }
  }

  public static int NON_EXISTING_ID = -1;
  protected S3ObjectCollection oc;
  protected long inodeId = NON_EXISTING_ID;

  public S3ObjectInfoContiguous(S3Object obj, long inodeId) {
    super(obj);
    this.inodeId = inodeId;
    if(obj instanceof S3ObjectInfoContiguous) {
      S3ObjectInfoContiguous casted = (S3ObjectInfoContiguous) obj;
      this.oc = casted.oc;
      if(inodeId != casted.inodeId) {
        throw new IllegalArgumentException("inodeId does not match");
      }
    }
  }

  public S3ObjectInfoContiguous() {
    this.oc = null;
  }

  protected S3ObjectInfoContiguous(S3ObjectInfoContiguous from) {
    super(from);
    this.oc = from.oc;
    this.inodeId = from.inodeId;
  }

  public S3ObjectCollection getObjectCollection() throws StorageException, TransactionContextException {
    S3ObjectCollection oc = (S3ObjectCollection) EntityManager.find(INodeFile.Finder.ByINodeIdFTIS, inodeId);
    this.oc = oc;
    if(oc == null) {
      this.inodeId = NON_EXISTING_ID;
    }
    return oc;
  }

  public void setObjectCollection(S3ObjectCollection oc) throws StorageException, TransactionContextException {
    this.oc = oc;
    if(oc != null) {
      setINodeId(oc.getId());
    }
  }

  public boolean isDeleted() throws StorageException, TransactionContextException {
    if(oc != null) {
      return false;
    } else {
      getObjectCollection();
    }
    return oc == null;
  }

  public long getInodeId() {
    return inodeId;
  }

  public void setINodeIdNoPersistance(long inodeId) {
    this.inodeId = inodeId;
  }

  public void setINodeId(long id)
          throws StorageException, TransactionContextException {
    setINodeIdNoPersistance(id);
    save();
  }

  public void setObjectIndex(int index) throws StorageException, TransactionContextException {
    setObjectIndexNoPersistence(index);
    save();
  }

  protected void save()
          throws StorageException, TransactionContextException {
    EntityManager.update(this);
  }

  public void remove()
          throws StorageException, TransactionContextException {
    EntityManager.remove(this);
  }

  public static S3ObjectInfoContiguous cloneObject(S3ObjectInfoContiguous object) throws StorageException {
    if (object == null){
      throw new StorageException("Unable to create a clone of the S3Object");
    }
    return new S3ObjectInfoContiguous(object, object.getInodeId());
  }
}
