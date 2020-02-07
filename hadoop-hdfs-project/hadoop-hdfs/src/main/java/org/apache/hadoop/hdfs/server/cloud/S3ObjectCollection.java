package org.apache.hadoop.hdfs.server.cloud;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;

@InterfaceAudience.Private
public interface S3ObjectCollection {
  S3ObjectInfoContiguous getLastObject() throws IOException;
  int numObjects() throws StorageException, TransactionContextException;
  S3ObjectInfoContiguous[] getObjects() throws IOException;
  S3ObjectInfoContiguous getObject(int index) throws IOException;
  void setObject(int index, S3ObjectInfoContiguous obj) throws IOException;
  long getId();
}
