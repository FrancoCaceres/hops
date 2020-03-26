package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**************************************************
 * An S3Object is a HopsFS primitive, identified by a
 * long.
 * <p/>
 * ************************************************
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3Object implements Writable, Comparable<S3Object> {

  static {                                      // register a ctor
    WritableFactories.setFactory(S3Object.class, new WritableFactory() {
      @Override
      public Writable newInstance() {
        return new S3Object();
      }
    });
  }

  private static long NON_EXISTING_OBJ_ID = Long.MIN_VALUE;
  private long objectId;
  private int objectIndex;
  private String region;
  private String bucket;
  private String key;
  private String versionId;
  private long numBytes;
  private long checksum;

  public S3Object() {
    this(NON_EXISTING_OBJ_ID, -1, "", "", "", "", -1, -1);
  }

  public S3Object(final long objectId, final int objectIndex, final String region, final String bucket,
                  final String key, final String versionId, final long numBytes, final long checksum) {
    this.objectId = objectId;
    this.objectIndex = objectIndex;
    this.region = region;
    this.bucket = bucket;
    this.key = key;
    this.versionId = versionId;
    this.numBytes = numBytes;
    this.checksum = checksum;
  }

  public S3Object(S3Object from) {
    this(from.objectId, from.objectIndex, from.region, from.bucket, from.key, from.versionId, from.numBytes,
            from.checksum);
  }

  @Override
  public int compareTo(S3Object other) {
    return objectIndex < other.objectIndex ? -1 : objectIndex > other.objectIndex ? 1 : 0;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeHelper(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    readHelper(in);
  }

  final void writeHelper(DataOutput out) throws IOException {
    out.writeLong(objectId);
    out.writeLong(objectIndex);
    out.writeUTF(region);
    out.writeUTF(bucket);
    out.writeUTF(key);
    out.writeUTF(versionId);
    out.writeLong(numBytes);
    out.writeLong(checksum);
  }

  final void readHelper(DataInput in) throws IOException {
    this.objectId = in.readLong();
    this.objectIndex = in.readInt();
    this.region = in.readUTF();
    this.bucket = in.readUTF();
    this.key = in.readUTF();
    this.versionId = in.readUTF();
    this.numBytes = in.readLong();
    this.checksum = in.readLong();
    if (numBytes < 0) {
      throw new IOException("Unexpected s3Object size: " + numBytes);
    }
  }

  @Override // Object
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof S3Object)) {
      return false;
    }
    return compareTo((S3Object) o) == 0;
  }

  public long getObjectId() {
    return objectId;
  }

  public void setObjectIdNoPersistence(long objectId) {
    this.objectId = objectId;
  }

  public int getObjectIndex() {
    return objectIndex;
  }

  public void setObjectIndexNoPersistence(int objectIndex) {
    this.objectIndex = objectIndex;
  }

  public String getRegion() {
    return region;
  }

  public void setRegionNoPersistence(String region) {
    this.region = region;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucketNoPersistence(String bucket) {
    this.bucket = bucket;
  }

  public String getKey() {
    return key;
  }

  public void setKeyNoPersistence(String key) {
    this.key = key;
  }

  public String getVersionId() {
    return versionId;
  }

  public void setVersionIdNoPersistence(String versionId) {
    this.versionId = versionId;
  }

  public long getNumBytes() {
    return numBytes;
  }

  public void setNumBytesNoPersistence(long numBytes) {
    this.numBytes = numBytes;
  }

  public long getChecksum() {
    return checksum;
  }

  public void setChecksumNoPersistence(long checksum) {
    this.checksum = checksum;
  }

  @Override
  public String toString() {
    return "S3Object{" +
            "objectId=" + objectId +
            ", objectIndex=" + objectIndex +
            ", region='" + region + '\'' +
            ", bucket='" + bucket + '\'' +
            ", key='" + key + '\'' +
            ", versionId='" + versionId + '\'' +
            ", numBytes=" + numBytes +
            '}';
  }
}
