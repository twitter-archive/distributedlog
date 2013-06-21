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

package com.twitter.distributedlog;

import java.io.*;

/**
 * A reusable {@link DataOutput} implementation that writes to an in-memory
 * buffer.
 *
 * <p>This makes more optimal use of memory as opposed to creating a DataOutputStream and
 * ByteArrayOutputStream for every write.
 *
 */
public class DataOutputBuffer extends DataOutputStream {

  private static class Buffer extends ByteArrayOutputStream {
    public byte[] getData() { return buf; }
    public int getLength() { return count; }

    public Buffer() {
      super();
    }
    
    public Buffer(int size) {
      super(size);
    }
    
    public void write(DataInput in, int len) throws IOException {
      int newcount = count + len;
      if (newcount > buf.length) {
        byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }
      in.readFully(buf, count, len);
      count = newcount;
    }
  }

  private Buffer buffer;
  
  /** Constructs a new empty buffer. */
  public DataOutputBuffer() {
    this(new Buffer());
  }
  
  public DataOutputBuffer(int size) {
    this(new Buffer(size));
  }
  
  private DataOutputBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  /** Returns the current contents of the buffer.
   *  Data is only valid to {@link #getLength()}.
   */
  public byte[] getData() { return buffer.getData(); }

  /** Returns the length of the valid data currently in the buffer. */
  public int getLength() { return buffer.getLength(); }

  /** Resets the buffer to empty. */
  public DataOutputBuffer reset() {
    this.written = 0;
    buffer.reset();
    return this;
  }

  /** Writes bytes from a DataInput directly into the buffer. */
  public void write(DataInput in, int length) throws IOException {
    buffer.write(in, length);
  }

  /** Write to a file stream */
  public void writeTo(OutputStream out) throws IOException {
    buffer.writeTo(out);
  }
}
