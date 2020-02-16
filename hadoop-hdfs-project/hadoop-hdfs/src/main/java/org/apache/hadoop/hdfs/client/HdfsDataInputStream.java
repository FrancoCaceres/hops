/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.S3DFSInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * The Hdfs implementation of {@link FSDataInputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HdfsDataInputStream extends FSDataInputStream {
  public HdfsDataInputStream(DFSInputStream in) throws IOException {
    super(in);
  }

  public HdfsDataInputStream(S3DFSInputStream in) throws IOException {
    super(in);
  }
  
 public HdfsDataInputStream(CryptoInputStream in) throws IOException {
    super(in);
    Preconditions.checkArgument(in.getWrappedStream() instanceof DFSInputStream,
        "CryptoInputStream should wrap a DFSInputStream");
  }

  private DFSInputStream getDFSInputStream() {
    if (in instanceof CryptoInputStream) {
      return (DFSInputStream) ((CryptoInputStream) in).getWrappedStream();
    } else if(in instanceof DFSInputStream) {
      return (DFSInputStream) in;
    }
    return null;
  }

  /**
   * Get a reference to the wrapped output stream. We always want to return the
   * actual underlying InputStream, even when we're using a CryptoStream. e.g.
   * in the delegated methods below.
   *
   * @return the underlying output stream
   */
  public InputStream getWrappedStream() {
      return in;
  }

  /**
   * Get the datanode from which the stream is currently reading.
   */
  public DatanodeInfo getCurrentDatanode() {
    if(getDFSInputStream() != null) {
      return getDFSInputStream().getCurrentDatanode();
    }
    return null;
  }

  /**
   * Get the block containing the target position.
   */
  public ExtendedBlock getCurrentBlock() {
    if(getDFSInputStream() != null) {
      return getDFSInputStream().getCurrentBlock();
    }
    return null;
  }

  /**
   * Get the collection of blocks that has already been located.
   */
  public List<LocatedBlock> getAllBlocks() throws IOException {
    if(getDFSInputStream() != null) {
      return getDFSInputStream().getAllBlocks();
    }
    return null;
  }

  /**
   * Get the visible length of the file. It will include the length of the last
   * block even if that is in UnderConstruction state.
   *
   * @return The visible length of the file.
   */
  public long getVisibleLength() throws IOException {
    if(getDFSInputStream() != null) {
      return getDFSInputStream().getFileLength();
    } else if(in instanceof S3DFSInputStream) {
      return ((S3DFSInputStream) in).getFileLength();
    }
    return 0;
  }
  
  /**
   * Get statistics about the reads which this DFSInputStream has done.
   * Note that because HdfsDataInputStream is buffered, these stats may
   * be higher than you would expect just by adding up the number of
   * bytes read through HdfsDataInputStream.
   */
  public DFSInputStream.ReadStatistics getReadStatistics() {
    if(getDFSInputStream() != null) {
      return getDFSInputStream().getReadStatistics();
    } else if(in instanceof S3DFSInputStream) {
      return ((S3DFSInputStream) in).getReadStatistics();
    }
    return null;
  }

  public void clearReadStatistics() {
    if(getDFSInputStream() != null) {
      getDFSInputStream().clearReadStatistics();
    } else if(in instanceof S3DFSInputStream) {
      ((S3DFSInputStream) in).getReadStatistics();
    }
  }
}
