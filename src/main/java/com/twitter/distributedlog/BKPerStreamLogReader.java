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

import com.twitter.distributedlog.exceptions.DLIllegalStateException;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

/**
 * Input stream which reads from a BookKeeper ledger.
 */
class BKPerStreamLogReader {
    static final Logger LOG = LoggerFactory.getLogger(BKPerStreamLogReader.class);

    private final long firstTxId;
    private final int logVersion;
    protected boolean inProgress;
    protected LedgerDescriptor ledgerDescriptor;
    protected LedgerDataAccessor ledgerDataAccessor;
    protected boolean isExhausted;
    private LogRecord currLogRec;
    private final boolean dontSkipControl;
    protected final StatsLogger statsLogger;

    protected LedgerInputStream lin;
    protected LogRecord.Reader reader;

    protected BKPerStreamLogReader(final LogSegmentLedgerMetadata metadata, StatsLogger statsLogger) {
        this.firstTxId = metadata.getFirstTxId();
        this.logVersion = metadata.getVersion();
        this.inProgress = metadata.isInProgress();
        this.statsLogger = statsLogger;
        this.isExhausted = false;
        this.dontSkipControl = false;
    }

    /**
     * Construct BookKeeper edit log input stream.
     * Starts reading from firstBookKeeperEntry. This allows the stream
     * to take a shortcut during recovery, as it doesn't have to read
     * every edit log transaction to find out what the last one is.
     */
    BKPerStreamLogReader(LedgerDescriptor desc, LogSegmentLedgerMetadata metadata,
                         long firstBookKeeperEntry, LedgerDataAccessor ledgerDataAccessor, boolean dontSkipControl, StatsLogger statsLogger)
        throws IOException {
        this.firstTxId = metadata.getFirstTxId();
        this.logVersion = metadata.getVersion();
        this.inProgress = metadata.isInProgress();
        this.dontSkipControl = dontSkipControl;
        this.statsLogger = statsLogger;
        this.isExhausted = false;
        positionInputStream(desc, ledgerDataAccessor, firstBookKeeperEntry);
    }

    protected synchronized void positionInputStream(LedgerDescriptor desc, LedgerDataAccessor ledgerDataAccessor,
                                                    long firstBookKeeperEntry)
        throws IOException {
        this.lin = new LedgerInputStream(desc, ledgerDataAccessor, firstBookKeeperEntry, statsLogger);
        this.reader = new LogRecord.Reader(new DataInputStream(
            new BufferedInputStream(lin,
                // Size the buffer only as much look ahead we need for skipping
                DistributedLogConstants.INPUTSTREAM_MARK_LIMIT)),
            logVersion);
        // Note: The caller of the function (or a derived class is expected to open the
        // LedgerDescriptor and pass the ownership to the BKPerStreamLogReader
        this.ledgerDescriptor = desc;
        this.ledgerDataAccessor = ledgerDataAccessor;
    }

    protected synchronized void resetExhausted() {
        this.isExhausted = false;
    }

    protected synchronized LedgerDescriptor getLedgerDescriptor() {
        return this.ledgerDescriptor;
    }

    protected synchronized LedgerDataAccessor getLedgerDataAccessor() {
        return this.ledgerDataAccessor;
    }

    public long getFirstTxId() {
        return firstTxId;
    }

    public LogRecord readOp(boolean nonBlocking) throws IOException {
        boolean oldValue = lin.isNonBlocking();
        lin.setNonBlocking(nonBlocking);
        LogRecord toRet = null;
        if (!isExhausted) {
            do {
                toRet = reader.readOp();
                isExhausted = (toRet == null);
            } while ((toRet != null) && !dontSkipControl && toRet.isControl());
        }
        lin.setNonBlocking(oldValue);
        return toRet;
    }

    public void close() throws IOException {
        try {
            getLedgerDataAccessor().closeLedger(getLedgerDescriptor());
        } catch (Throwable t) {
          // we caught all the potential exceptions when closing
          // {@link https://jira.twitter.biz/browse/PUBSUB-1146}
          LOG.error("Exception closing ledger {} : ", ledgerDescriptor, t);
        }
    }


    public long length() throws IOException {
        return getLedgerDataAccessor().getLength(getLedgerDescriptor());
    }

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
        isExhausted = !reader.skipTo(txId);
        return !isExhausted;
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
        private boolean nonBlocking;
        private static Counter getWithNoWaitCount = null;
        private static Counter getWithWaitCount = null;
        private static OpStatsLogger getWithNoWaitStat = null;
        private static OpStatsLogger getWithWaitStat = null;
        private static Counter illegalStateCount = null;

        /**
         * Construct ledger input stream
         *
         * @param ledgerDesc ledger descriptor
         * @param ledgerDataAccessor ledger data accessor
         * @param firstBookKeeperEntry ledger entry to start reading from
         */
        LedgerInputStream(LedgerDescriptor ledgerDesc, LedgerDataAccessor ledgerDataAccessor,
                          long firstBookKeeperEntry, StatsLogger statsLogger) {
            this.ledgerDesc = ledgerDesc;
            readEntries = firstBookKeeperEntry;
            this.ledgerDataAccessor = ledgerDataAccessor;

            StatsLogger getEntryStatsLogger = statsLogger.scope("get_entry");

            if (null == getWithWaitCount) {
                getWithWaitCount = getEntryStatsLogger.getCounter("block");
            }

            if (null == getWithNoWaitCount) {
                getWithNoWaitCount = getEntryStatsLogger.getCounter("no_block");
            }

            if (null == getWithNoWaitStat) {
                getWithNoWaitStat = getEntryStatsLogger.getOpStatsLogger("no_block_latency");
            }

            if (null == getWithWaitStat) {
                getWithWaitStat = getEntryStatsLogger.getOpStatsLogger("block_latency");
            }

            if (null == illegalStateCount) {
                illegalStateCount = statsLogger.getCounter("illegal_state");
            }
        }

        private Counter getEntryCounter(boolean nonBlocking) {
            if (nonBlocking) {
                return getWithNoWaitCount;
            } else {
                return getWithWaitCount;
            }
        }

        private OpStatsLogger getEntryLatencyStat(boolean nonBlocking) {
            if (nonBlocking) {
                return getWithNoWaitStat;
            } else {
                return getWithWaitStat;
            }
        }

        /**
         * Get input stream representing next entry in the
         * ledger.
         *
         * @return input stream, or null if no more entries
         */
        private InputStream nextStream() throws IOException {
            long maxEntry = ledgerDataAccessor.getLastAddConfirmed(ledgerDesc);
            if (readEntries > maxEntry) {
                LOG.debug("Read Entries {} Max Entry {}", readEntries, maxEntry);
                return null;
            }

            LedgerReadPosition readPosition = new LedgerReadPosition(ledgerDesc.getLedgerId(),
                                                    ledgerDesc.getLedgerSequenceNo(),
                                                    readEntries);
            LedgerEntry e;
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                getEntryCounter(nonBlocking).inc();
                e = ledgerDataAccessor.getEntry(ledgerDesc, readPosition, nonBlocking);
                getEntryLatencyStat(nonBlocking).registerSuccessfulEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } catch (IOException ioe) {
                getEntryLatencyStat(nonBlocking).registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                throw ioe;
            }

            if (null == e) {
                if (nonBlocking) {
                    LOG.debug("Read Entries {} Max Entry {}, Nothing in the cache", readEntries, maxEntry);
                }
                return null;
            }

            ledgerDataAccessor.remove(readPosition);
            readEntries++;
            return e.getEntryInputStream();
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
                return doRead(b, off, len);
            } catch (ArrayIndexOutOfBoundsException e) {
                illegalStateCount.inc();
                StringBuilder sb = new StringBuilder();
                sb.append("read(array=").append(b.length).append(", off=").append(off)
                  .append(", len=").append(len).append("), remaining bytes in entry ")
                  .append(readEntries).append(" of ").append(ledgerDesc).append(" is ");
                if (entryStream instanceof ChannelBufferInputStream) {
                    ChannelBufferInputStream cbis = (ChannelBufferInputStream) entryStream;
                    sb.append(cbis.available());
                } else {
                    sb.append("null");
                }
                throw new DLIllegalStateException("IllegalState on " + sb.toString(), e);
            }
        }

        private int doRead(byte[] b, int off, int len) throws IOException {
            int read = 0;
            if (entryStream == null) {
                entryStream = nextStream();
                if (entryStream == null) {
                    return read;
                }
            }

            while (read < len) {
                int thisread = entryStream.read(b, off + read, (len - read));
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
        }

        public void setNonBlocking(boolean nonBlocking) {
            this.nonBlocking = nonBlocking;
        }

        public boolean isNonBlocking() {
            return nonBlocking;
        }

        long nextEntryToRead() {
            return readEntries;
        }

        public void setLedgerDataAccessor(LedgerDataAccessor ledgerDataAccessor) {
            this.ledgerDataAccessor = ledgerDataAccessor;
        }

        public boolean reachedEndOfLedger() {
            try {
                long maxEntry = ledgerDataAccessor.getLastAddConfirmed(ledgerDesc);
                return (readEntries > maxEntry);
            } catch (IOException exc) {
                return false;
            }
        }
    }
}
