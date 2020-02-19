package org.apache.hadoop.hdfs.server.cloud;

import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;

@InterfaceAudience.Private
public interface S3ObjectCollection {
  S3ObjectInfoContiguous getLastS3Object() throws IOException;
  int numS3Objects() throws StorageException, TransactionContextException;
  S3ObjectInfoContiguous[] getS3Objects() throws IOException;
  S3ObjectInfoContiguous getS3Object(int index) throws IOException;
  void setS3Object(int index, S3ObjectInfoContiguous obj) throws IOException;
  long getId();
}
