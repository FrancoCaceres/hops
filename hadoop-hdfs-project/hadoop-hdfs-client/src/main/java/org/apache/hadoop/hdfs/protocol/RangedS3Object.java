package org.apache.hadoop.hdfs.protocol;

import java.util.ArrayList;
import java.util.List;

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

  // Not to be mistaken with last index of the whole S3 object
  public long getLastIndex() {
    return offset + length - 1;
  }

  // Get ranged part starting at [off] bytes from the start of this (whole) S3 object.
  public RangedPart getRangedPart(long off, int length, long partSize) {
    int partNumber = (int)(off/partSize) + 1; // guaranteed to be [1-10000]
    int skippedParts = partNumber - 1;
    long skippedPartsBytes = skippedParts * partSize;
    long startIndexOfNextPart = partNumber * partSize;
    long bytesToReadInCurrentPart = Math.min(startIndexOfNextPart, getLastIndex() + 1) - off;
    long rangedPartOffset = off - skippedPartsBytes;
    int rangedPartLength = (int)Math.min(length, partSize);
    rangedPartLength = (int)Math.min(rangedPartLength, bytesToReadInCurrentPart);
    return new RangedPart(partNumber, rangedPartOffset, rangedPartLength);
  }

  public List<RangedPart> getRangedParts(long off, int length, long partSize) {
    List<RangedPart> rangedParts = new ArrayList<>();
    long rangedObjectLength = getLength();
    long toRead = Math.min(length,rangedObjectLength);
    while(toRead > 0) {
      RangedPart rangedPart = getRangedPart(off, length, partSize);
      rangedParts.add(rangedPart);
      off += rangedPart.length;
      length -= rangedPart.length;
      toRead -= rangedPart.length;
    }
    return rangedParts;
  }

  public List<RangedPart> getAllRangedParts(long partSize) {
    List<RangedPart> rangedParts = new ArrayList<>();
    long toRead = length;
    long off = offset;
    int length = Integer.MAX_VALUE;
    while(toRead > 0) {
      RangedPart rangedPart = getRangedPart(off, (int)Math.min(length, toRead), partSize);
      rangedParts.add(rangedPart);
      off += rangedPart.length;
      toRead -= rangedPart.length;
    }
    return rangedParts;
  }

  @Override
  public String toString() {
    return "RangedS3Object{" +
            "offset=" + offset +
            ", length=" + length +
            "} " + super.toString();
  }

  public static class RangedPart {
    private final int partNumber;
    private final long offset; // offset from the start of the given part
    private final int length; // how much is to be read from the offset

    public RangedPart(int partNumber, long offset, int length) {
      this.partNumber = partNumber;
      this.offset = offset;
      this.length = length;
    }

    public int getPartNumber() {
      return partNumber;
    }

    public long getOffset() {
      return offset;
    }

    public int getLength() {
      return length;
    }
  }
}
