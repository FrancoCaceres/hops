package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.fs.FileEncryptionInfo;

import java.util.ArrayList;
import java.util.List;

public class S3File {
  private long fileLength;
  private List<S3Object> objects;
  private boolean underConstruction; // TODO FCG Do we really need this?
  private FileEncryptionInfo fileEncryptionInfo = null;
  private byte[] data;

  public S3File() {
    this.fileLength = 0;
    this.objects = null;
    this.underConstruction = false;
    this.fileEncryptionInfo = null;
    this.data = null;
  }

  public S3File(long fileLength, List<S3Object> objects, boolean underConstruction,
                FileEncryptionInfo fileEncryptionInfo, byte[] data) {
    this.fileLength = fileLength;
    this.objects = objects;
    this.underConstruction = underConstruction;
    this.fileEncryptionInfo = fileEncryptionInfo;
    this.data = data;
  }

  public long getFileLength() {
    return fileLength;
  }

  public void setFileLength(long fileLength) {
    this.fileLength = fileLength;
  }

  public List<S3Object> getObjects() {
    return objects;
  }

  public void setObjects(List<S3Object> objects) {
    this.objects = objects;
  }

  public boolean isUnderConstruction() {
    return underConstruction;
  }

  public void setUnderConstruction(boolean underConstruction) {
    this.underConstruction = underConstruction;
  }

  public FileEncryptionInfo getFileEncryptionInfo() {
    return fileEncryptionInfo;
  }

  public void setFileEncryptionInfo(FileEncryptionInfo fileEncryptionInfo) {
    this.fileEncryptionInfo = fileEncryptionInfo;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public List<RangedS3Object> getObjectsInRange(long start, long end) { // [start:end]
    List<RangedS3Object> res = new ArrayList<>();
    long pos = 0;
    for(S3Object obj : objects) {
      long objStart = pos; // Index of the whole file
      long objEnd = pos + obj.getNumBytes() - 1; // Index of the whole file
      boolean objStartInside = objStart >= start && objStart <= end;
      boolean objEndInside = objEnd >= start && objEnd <= end;

      // Case 1: object partially overlaps with requested range on its end side
      // --[####obj####]----------
      // --------[####range####]--
      if(!objStartInside && objEndInside) {
        res.add(new RangedS3Object(obj, start - objStart, objEnd - start + 1));
      }

      // Case 2: object is entirely within the requested range
      // -------[##obj##]---------
      // -----[####range####]-----
      if(objStartInside && objEndInside) {
        res.add(new RangedS3Object(obj, 0, obj.getNumBytes()));
      }

      // Case 3: object partially overlaps with requested range on its start side
      // -------[####obj####]------
      // --[####range####]---------
      if(objStartInside && !objEndInside) {
        res.add(new RangedS3Object(obj, 0, end - objStart + 1));
      }

      // Case 4: requested range is entirely within the object
      // ------[####obj####]-------
      // --------[#range#]---------
      if(start >= objStart && start <= objEnd && end >= objStart && end <= objEnd) {
        res.add(new RangedS3Object(obj, start - objStart, end - start + 1));
      }
      pos += obj.getNumBytes();
    }
    return res;
  }
}
