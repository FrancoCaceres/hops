package org.apache.hadoop.hdfs.protocol;

public class RangedS3Object extends S3Object {
  private final long offset;
  private final long length;

  public RangedS3Object(S3Object from, final long offset, final long length) {
    super(from);
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public long getLastIndex() {
    return offset + length - 1;
  }
}
