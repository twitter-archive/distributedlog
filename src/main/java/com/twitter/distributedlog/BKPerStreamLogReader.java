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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BKException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Input stream which reads from a BookKeeper ledger.
 */
class BKPerStreamLogReader implements PerStreamLogReader {
  static final Logger LOG = LoggerFactory.getLogger(BKPerStreamLogReader.class);

  private final long firstTxId;
  protected long lastTxId;
  private final int logVersion;
  protected boolean inProgress;
  protected LedgerDescriptor ledgerDescriptor;
  protected LedgerDataAccessor ledgerDataAccessor;
  private LogRecord currLogRec;
  private final boolean dontSkipControl;

  protected LedgerInputStream lin;
  protected LogRecord.Reader reader;
  protected PositionTrackingInputStream tracker;

  protected BKPerStreamLogReader(final LogSegmentLedgerMetadata metadata) {
      this.firstTxId = metadata.getFirstTxId();
      this.lastTxId = metadata.getLastTxId();
      this.logVersion = metadata.getVersion();
      this.inProgress = metadata.isInProgress();
      this.dontSkipControl = false;
  }

  /**
   * Construct BookKeeper edit log input stream. 
   * Starts reading from firstBookKeeperEntry. This allows the stream
   * to take a shortcut during recovery, as it doesn't have to read
   * every edit log transaction to find out what the last one is.
   */
  BKPerStreamLogReader(LedgerDescriptor desc, LogSegmentLedgerMetadata metadata,
                       long firstBookKeeperEntry, LedgerDataAccessor ledgerDataAccessor,boolean dontSkipControl)
      throws IOException {
      this.firstTxId = metadata.getFirstTxId();
      this.lastTxId = metadata.getLastTxId();
      this.logVersion = metadata.getVersion();
      this.inProgress = metadata.isInProgress();
      this.dontSkipControl = dontSkipControl;
      positionInputStream(desc, ledgerDataAccessor, firstBookKeeperEntry);

  }
  protected void positionInputStream(LedgerDescriptor desc, long firstBookKeeperEntry)
      throws IOException {
      positionInputStream(desc, null, firstBookKeeperEntry);
  }

  protected void positionInputStream(LedgerDescriptor desc, LedgerDataAccessor ledgerDataAccessor, long firstBookKeeperEntry)
      throws IOException {
    this.lin = new LedgerInputStream(desc, ledgerDataAccessor, firstBookKeeperEntry);
    this.tracker = new PositionTrackingInputStream(new BufferedInputStream(lin));
    this.reader = new LogRecord.Reader(new DataInputStream(tracker), logVersion);

    do {
      currLogRec = reader.readOp();
    } while ((currLogRec != null) && !this.dontSkipControl && currLogRec.isControl());

    this.ledgerDescriptor = desc;
    this.ledgerDataAccessor = ledgerDataAccessor;
  }


  @Override
  public long getFirstTxId() throws IOException {
    return firstTxId;
  }

  @Override
  public long getLastTxId() throws IOException {
    return lastTxId;
  }
  
  @Override
  public int getVersion() throws IOException {
    return logVersion;
  }

  @Override
  public LogRecord readOp() throws IOException {
    LogRecord toRet = currLogRec;
    if (null != currLogRec) {
      do {
        currLogRec = reader.readOp();
      } while ((currLogRec != null) && !dontSkipControl && currLogRec.isControl());
    }
    return toRet;
  }

  @Override
  public void close() throws IOException {
    try {
        ledgerDataAccessor.closeLedger(ledgerDescriptor);
    } catch (Exception e) {
      throw new IOException("Exception closing ledger", e);
    }
  }

  @Override
  public long getPosition() {
    return tracker.getPos();
  }

  @Override
  public long length() throws IOException {
    return ledgerDataAccessor.getLength(ledgerDescriptor);
  }

  @Override
  public boolean isInProgress() {
    return inProgress;
  }

  /**
   * Skip forward to specified transaction id.
   * Currently we do this by just iterating forward.
   * If this proves to be too expensive, this can be reimplemented
   * with a binary search over bk entries
   */
  public boolean skipTo(long txId) throws IOException {
    while ((null!= currLogRec) && (currLogRec.getTransactionId() < txId)) {
        currLogRec = reader.readOp();
    }

    return (null != currLogRec);
  }

  /**
   * Input stream implementation which can be used by 
   * LogRecord.Reader
   */
  protected static class LedgerInputStream extends InputStream {
    private long readEntries;
    private InputStream entryStream = null;
    private final LedgerDescriptor ledgerDesc;
    private LedgerDataAccessor ledgerDataAccessor;

    /**
     * Construct ledger input stream
     * @param lh the ledger handle to read from
     * @param firstBookKeeperEntry ledger entry to start reading from
     */
    LedgerInputStream(LedgerDescriptor ledgerDesc, LedgerDataAccessor ledgerDataAccessor, long firstBookKeeperEntry)
        throws IOException {
      this.ledgerDesc = ledgerDesc;
      readEntries = firstBookKeeperEntry;
      this.ledgerDataAccessor = ledgerDataAccessor;
    }

    /**
     * Get input stream representing next entry in the
     * ledger.
     * @return input stream, or null if no more entries
     */
    private InputStream nextStream() throws IOException {
        try {
        long maxEntry = ledgerDataAccessor.getLastAddConfirmed(ledgerDesc);
        if (readEntries > maxEntry) {
            LOG.debug("Read Entries {} Max Entry {}", readEntries, maxEntry);
            return null;
        }

        LedgerReadPosition readPosition = new LedgerReadPosition(ledgerDesc.getLedgerId(), readEntries);
        LedgerEntry e = ledgerDataAccessor.getWithWait(ledgerDesc, readPosition);
        assert (e != null);
        ledgerDataAccessor.remove(readPosition);
        readEntries++;
        return e.getEntryInputStream();
      } catch (BKException bke) {
        LOG.info("Reached the end of the stream", bke);
      } catch (Exception e) {
        throw new IOException("Error reading entries from bookkeeper", e);
      }
      return null;
    }

    @Override
    public int read() throws IOException {
      byte[] b = new byte[1];
      if (read(b, 0, 1) != 1) {
        return -1;
      } else {
        return b[0];
      }
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
        int read = 0;
        if (entryStream == null) {
          entryStream = nextStream();
          if (entryStream == null) {
            return read;
          }
        }

        while (read < len) {
          int thisread = entryStream.read(b, off+read, (len-read));
          if (thisread == -1) {
            entryStream = nextStream();
            if (entryStream == null) {
              return read;
            }
          } else {
            read += thisread;
          }
        }
        return read;
      } catch (IOException e) {
        throw e;
      }

    }

    long nextEntryToRead() {
        return readEntries;
    }

    public void setLedgerDataAccessor(LedgerDataAccessor ledgerDataAccessor) {
        this.ledgerDataAccessor = ledgerDataAccessor;
    }
  }

    /**
     * Stream wrapper that keeps track of the current stream position.
     */
    protected static class PositionTrackingInputStream extends FilterInputStream {
        private long curPos = 0;
        private long markPos = -1;

        public PositionTrackingInputStream(InputStream is) {
            super(is);
        }

        public int read() throws IOException {
            int ret = super.read();
            if (ret != -1) curPos++;
            return ret;
        }

        public int read(byte[] data) throws IOException {
            int ret = super.read(data);
            if (ret > 0) curPos += ret;
            return ret;
        }

        public int read(byte[] data, int offset, int length) throws IOException {
            int ret = super.read(data, offset, length);
            if (ret > 0) curPos += ret;
            return ret;
        }

        public void mark(int limit) {
            super.mark(limit);
            markPos = curPos;
        }

        public void reset() throws IOException {
            if (markPos == -1) {
                throw new IOException("Not marked!");
            }
            super.reset();
            curPos = markPos;
            markPos = -1;
        }

        public long getPos() {
            return curPos;
        }
    }

}
