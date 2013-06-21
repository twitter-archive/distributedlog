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

import java.io.Closeable;
import java.io.IOException;

/**
 * A generic abstract class to support reading edits log data from 
 * persistent storage.
 * 
 * It should stream bytes from the storage exactly as they were written
 * into the #{@link PerStreamLogWriter}.
 */
public interface PerStreamLogReader extends Closeable {
  /** 
   * @return the first transaction which will be found in this stream
   */
  public long getFirstTxId() throws IOException;
  
  /** 
   * @return the last transaction which will be found in this stream
   */
  public long getLastTxId() throws IOException;


  /**
   * Close the stream.
   * @throws IOException if an error occurred while closing
   */
  public void close() throws IOException;

  /** 
   * Read an operation from the stream
   * @return an operation from the stream or null if at end of stream
   * @throws IOException if there is an error reading from the stream
   */
  public LogRecord readOp() throws IOException;

  /** 
   * Get the layout version of the data in the stream.
   * @return the layout version of the ops in the stream.
   * @throws IOException if there is an error reading the version
   */
  public int getVersion() throws IOException;

  /**
   * Get the "position" of in the stream. This is useful for 
   * debugging and operational purposes.
   *
   * Different stream types can have a different meaning for 
   * what the position is. For file streams it means the byte offset
   * from the start of the file.
   *
   * @return the position in the stream
   */
  public long getPosition();

  /**
   * Return the size of the current edits log.
   */
  public long length() throws IOException;
  
  /**
   * Return true if this stream is in progress, false if it is finalized.
   */
  public boolean isInProgress();
}
