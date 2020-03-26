package io.hops.metadata.adaptor;

import io.hops.exception.StorageException;
import io.hops.metadata.DalAdaptor;
import io.hops.metadata.hdfs.dal.S3ObjectInfoDataAccess;
import io.hops.metadata.hdfs.entity.S3ObjectInfo;
import org.apache.hadoop.hdfs.protocol.S3Object;
import org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class S3ObjectInfoDALAdaptor extends
        DalAdaptor<org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous, S3ObjectInfo>
        implements
        S3ObjectInfoDataAccess<org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous> {

  private final S3ObjectInfoDataAccess<S3ObjectInfo> dataAccess;

  public S3ObjectInfoDALAdaptor(S3ObjectInfoDataAccess<S3ObjectInfo> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public S3ObjectInfo convertHDFStoDAL(S3ObjectInfoContiguous hdfsClass) {
    if(hdfsClass != null) {
      S3ObjectInfo hopS3ObjInfo =
              new S3ObjectInfo(hdfsClass.getObjectId(), hdfsClass.getObjectIndex(), hdfsClass.getInodeId(),
                      hdfsClass.getRegion(), hdfsClass.getBucket(), hdfsClass.getKey(), hdfsClass.getVersionId(),
                      hdfsClass.getNumBytes(), hdfsClass.getChecksum());
      return hopS3ObjInfo;
    } else {
      return null;
    }
  }

  @Override
  public S3ObjectInfoContiguous convertDALtoHDFS(S3ObjectInfo dalClass) {
    if(dalClass == null) {
      return null;
    }
    S3Object obj = new S3Object(dalClass.getObjectId(), dalClass.getObjectIndex(), dalClass.getRegion(),
            dalClass.getBucket(), dalClass.getKey(), dalClass.getVersionId(), dalClass.getNumBytes(),
            dalClass.getChecksum());

    return new S3ObjectInfoContiguous(obj, dalClass.getInodeId());
  }

  @Override
  public int countAll() throws StorageException {
    return dataAccess.countAll();
  }

  @Override
  public S3ObjectInfoContiguous findById(long objectId, long inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findById(objectId, inodeId));
  }

  @Override
  public List<S3ObjectInfoContiguous> findByInodeId(long inodeId) throws StorageException {
    return (List<S3ObjectInfoContiguous>)convertDALtoHDFS(dataAccess.findByInodeId(inodeId));
  }

  @Override
  public List<S3ObjectInfoContiguous> findByInodeIds(long[] inodeIds) throws StorageException {
    return (List<S3ObjectInfoContiguous>)convertDALtoHDFS(dataAccess.findByInodeIds(inodeIds));
  }

  @Override
  public List<S3ObjectInfoContiguous> findAllS3Objects() throws StorageException {
    return (List<S3ObjectInfoContiguous>)convertDALtoHDFS(dataAccess.findAllS3Objects());
  }

  @Override
  public List<S3ObjectInfoContiguous> findByIds(long[] objectIds, long[] inodeIds) throws StorageException {
    return (List<S3ObjectInfoContiguous>)convertDALtoHDFS(dataAccess.findByIds(objectIds, inodeIds));
  }

  @Override
  public void add(S3ObjectInfoContiguous s3ObjectInfoContiguous) throws StorageException {
    dataAccess.add(convertHDFStoDAL(s3ObjectInfoContiguous));
  }

  @Override
  public void deleteAll(List<S3ObjectInfoContiguous> objects) throws StorageException {
    List<S3ObjectInfo> dals = new ArrayList<>(objects.size());
    for(S3ObjectInfoContiguous hdfs : objects) {
      dals.add(convertHDFStoDAL(hdfs));
    }
    dataAccess.deleteAll(dals);
  }

  @Override
  public void prepare(Collection<S3ObjectInfoContiguous> removed, Collection<S3ObjectInfoContiguous> newed,
                      Collection<S3ObjectInfoContiguous> modified) throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed), convertHDFStoDAL(modified));
  }
}
