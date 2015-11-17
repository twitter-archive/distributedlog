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
package com.twitter.distributedlog.v2;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.exceptions.DLIllegalStateException;
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

    private final String fullyQualifiedName;
    private final int logVersion;
    protected boolean inProgress;
    protected LedgerDescriptor ledgerDescriptor;
    protected LedgerDataAccessor ledgerDataAccessor;
    protected boolean isExhausted;
    private final boolean dontSkipControl;
    protected final StatsLogger statsLogger;
    protected boolean enableTrace;
    private Long skipThreshold = null;

    protected LedgerInputStream lin;
    protected LogRecord.Reader reader;

    protected BKPerStreamLogReader(BKLogPartitionHandler handler, final LogSegmentLedgerMetadata metadata, StatsLogger statsLogger) {
        this.fullyQualifiedName = handler.getFullyQualifiedName();
        this.logVersion = metadata.getVersion();
        this.inProgress = metadata.isInProgress();
        this.statsLogger = statsLogger;
        this.isExhausted = false;
        this.enableTrace = false;
        this.dontSkipControl = false;
    }

    /**
     * Construct BookKeeper edit log input stream.
     * Starts reading from firstBookKeeperEntry. This allows the stream
     * to take a shortcut during recovery, as it doesn't have to read
     * every edit log transaction to find out what the last one is.
     */
    BKPerStreamLogReader(BKLogPartitionHandler handler, LedgerDescriptor desc, LogSegmentLedgerMetadata metadata,
                         long firstBookKeeperEntry, LedgerDataAccessor ledgerDataAccessor, boolean dontSkipControl, StatsLogger statsLogger)
        throws IOException {
        this.fullyQualifiedName = handler.getFullyQualifiedName();
        this.logVersion = metadata.getVersion();
        this.inProgress = metadata.isInProgress();
        this.dontSkipControl = dontSkipControl;
        this.statsLogger = statsLogger;
        this.isExhausted = false;
        this.enableTrace = false;
        positionInputStream(desc, ledgerDataAccessor, firstBookKeeperEntry);
    }

    protected synchronized void positionInputStream(LedgerDescriptor desc, LedgerDataAccessor ledgerDataAccessor,
                                                    long firstBookKeeperEntry)
        throws IOException {
        this.lin = new LedgerInputStream(fullyQualifiedName, logVersion, desc, ledgerDataAccessor, firstBookKeeperEntry, statsLogger, enableTrace);
        this.reader = new LogRecord.Reader(this.lin, new DataInputStream(
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

    public synchronized LogRecordWithDLSN readOp(boolean nonBlocking) throws IOException {
        boolean oldValue = lin.isNonBlocking();
        lin.setNonBlocking(nonBlocking);
        LogRecordWithDLSN toRet = null;
        if (!isExhausted) {
            do {
                toRet = reader.readOp();
                isExhausted = (toRet == null);
            } while ((toRet != null) &&
                    ((!dontSkipControl && toRet.isControl()) ||
                    (null != skipThreshold) && toRet.getTransactionId() < skipThreshold));

            if (null != toRet) {
                skipThreshold = null;
            }
        }
        lin.setNonBlocking(oldValue);
        return toRet;
    }

    public void setSkipThreshold(long skipThreshold) {
        this.skipThreshold = skipThreshold;
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
    public synchronized boolean skipTo(long txId) throws IOException {
        isExhausted = !reader.skipTo(txId);
        return !isExhausted;
    }

    public synchronized void setEnableTrace(boolean enableTrace) {
        this.enableTrace = enableTrace;
        if (null != lin) {
            lin.setEnableTrace(enableTrace);
        }
    }

    synchronized boolean isTraceEnabled() {
        return enableTrace;
    }

    /**
     * Input stream implementation which can be used by
     * LogRecord.Reader
     */
    protected static class LedgerInputStream extends InputStream implements RecordStream {
        private long readEntries;
        private InputStream entryStream = null;
        LedgerReadPosition readPosition = null;
        private long currentSlotId = 0;
        private final int logVersion;
        private final LedgerDescriptor ledgerDesc;
        private LedgerDataAccessor ledgerDataAccessor;
        private final String fullyQualifiedName;
        private boolean nonBlocking = false;

        // Stats
        private final StatsLogger getEntryStatsLogger;
        private static Counter getWithNoWaitCount = null;
        private static Counter getWithWaitCount = null;
        private static OpStatsLogger getWithNoWaitStat = null;
        private static OpStatsLogger getWithWaitStat = null;
        private static Counter illegalStateCount = null;
        private boolean enableTrace = false;

        /**
         * Construct ledger input stream
         *
         * @param ledgerDesc ledger descriptor
         * @param ledgerDataAccessor ledger data accessor
         * @param firstBookKeeperEntry ledger entry to start reading from
         */
        LedgerInputStream(String fullyQualifiedName, int logVersion,
                          LedgerDescriptor ledgerDesc, LedgerDataAccessor ledgerDataAccessor,
                          long firstBookKeeperEntry, StatsLogger statsLogger, boolean enableTrace) {
            this.fullyQualifiedName = fullyQualifiedName;
            this.logVersion = logVersion;
            this.ledgerDesc = ledgerDesc;
            readEntries = firstBookKeeperEntry;
            this.ledgerDataAccessor = ledgerDataAccessor;
            this.enableTrace = enableTrace;

            this.getEntryStatsLogger = statsLogger.scope("get_entry");
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

        private boolean shouldRemoveEnvelope() {
            return LogSegmentLedgerMetadata.supportsEnvelopedEntries(logVersion);
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

            readPosition = new LedgerReadPosition(ledgerDesc.getLedgerId(),
                                                    ledgerDesc.getLedgerSequenceNo(),
                                                    readEntries);
            LedgerEntry e;
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                getEntryCounter(nonBlocking).inc();
                e = ledgerDataAccessor.getEntry(ledgerDesc, readPosition, nonBlocking, enableTrace);
                getEntryLatencyStat(nonBlocking).registerSuccessfulEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            } catch (IOException ioe) {
                getEntryLatencyStat(nonBlocking).registerFailedEvent(
                    stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
                throw ioe;
            }

            if (null == e) {
                if (enableTrace) {
                    LOG.info("{}: Max Entry {}, getEntry returned null", this, maxEntry);
                } else if (nonBlocking) {
                    LOG.debug("{}: Max Entry {}, Nothing in the cache", this, maxEntry);
                }
                return null;
            }

            ledgerDataAccessor.remove(readPosition);
            readEntries++;
            InputStream ret = e.getEntryInputStream();
            if (shouldRemoveEnvelope()) {
                // Decompress here.
                ret = EnvelopedEntry.fromInputStream(ret, getEntryStatsLogger);
            }
            return ret;
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

        @Override
        public void advanceToNextRecord() {
            if (null == readPosition) {
                return;
            }
            currentSlotId++;
        }

        @Override
        public DLSN getCurrentPosition() {
            if (null == readPosition) {
                return DLSN.InvalidDLSN;
            }
            return new DLSN(ledgerDesc.getLedgerSequenceNo(), readPosition.getEntryId(), currentSlotId);
        }

        @Override
        public String getName() {
            return fullyQualifiedName;
        }

        public boolean reachedEndOfLedger() {
            try {
                long maxEntry = ledgerDataAccessor.getLastAddConfirmed(ledgerDesc);
                return (readEntries > maxEntry);
            } catch (IOException exc) {
                return false;
            }
        }

        @Override
        public String toString() {
            return String.format("Next Entry to read: %s, Ledger data accessor %s", readEntries, ledgerDataAccessor);
        }

        public void setEnableTrace(boolean enableTrace) {
            this.enableTrace = enableTrace;
        }
    }
}
