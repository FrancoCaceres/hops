package io.hops.transaction.lock;

import org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

abstract class BaseIndividualS3ObjectLock extends Lock {
  protected final List<S3ObjectInfoContiguous> objects;

  BaseIndividualS3ObjectLock() {
    this.objects = new ArrayList<>();
  }

  Collection<S3ObjectInfoContiguous> getObjects() {
    return objects;
  }

  @Override
  protected Type getType() {
    return Type.S3Object;
  }
}
