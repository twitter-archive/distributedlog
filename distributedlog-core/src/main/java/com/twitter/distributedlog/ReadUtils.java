package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

/**
 * Utility function for readers
 */
public class ReadUtils {

    static final Logger LOG = LoggerFactory.getLogger(ReadUtils.class);

    private static class ScanContext {
        // variables to about current scan state
        final AtomicInteger numEntriesToScan;
        final AtomicLong curStartEntryId;
        final AtomicLong curEndEntryId;

        // scan settings
        final long startEntryId;
        final long endEntryId;
        final int scanStartBatchSize;
        final int scanMaxBatchSize;
        final boolean includeControl;
        final boolean includeEndOfStream;
        final boolean backward;

        // number of records scanned
        final AtomicInteger numRecordsScanned;

        ScanContext(long startEntryId, long endEntryId,
                    int scanStartBatchSize,
                    int scanMaxBatchSize,
                    boolean includeControl,
                    boolean includeEndOfStream,
                    boolean backward,
                    AtomicInteger numRecordsScanned) {
            this.startEntryId = startEntryId;
            this.endEntryId = endEntryId;
            this.scanStartBatchSize = scanStartBatchSize;
            this.scanMaxBatchSize = scanMaxBatchSize;
            this.includeControl = includeControl;
            this.includeEndOfStream = includeEndOfStream;
            this.backward = backward;
            // Scan state
            this.numEntriesToScan = new AtomicInteger(scanStartBatchSize);
            if (backward) {
                this.curStartEntryId = new AtomicLong(
                        Math.max(startEntryId, (endEntryId - scanStartBatchSize + 1)));
                this.curEndEntryId = new AtomicLong(endEntryId);
            } else {
                this.curStartEntryId = new AtomicLong(startEntryId);
                this.curEndEntryId = new AtomicLong(
                        Math.min(endEntryId, (startEntryId + scanStartBatchSize - 1)));
            }
            this.numRecordsScanned = numRecordsScanned;
        }

        boolean moveToNextRange() {
            if (backward) {
                return moveBackward();
            } else {
                return moveForward();
            }
        }

        boolean moveBackward() {
            long nextEndEntryId = curStartEntryId.get() - 1;
            if (nextEndEntryId < startEntryId) {
                // no entries to read again
                return false;
            }
            curEndEntryId.set(nextEndEntryId);
            // update num entries to scan
            numEntriesToScan.set(
                    Math.min(numEntriesToScan.get() * 2, scanMaxBatchSize));
            // update start entry id
            curStartEntryId.set(Math.max(startEntryId, nextEndEntryId - numEntriesToScan.get() + 1));
            return true;
        }

        boolean moveForward() {
            long nextStartEntryId = curEndEntryId.get() + 1;
            if (nextStartEntryId > endEntryId) {
                // no entries to read again
                return false;
            }
            curStartEntryId.set(nextStartEntryId);
            // update num entries to scan
            numEntriesToScan.set(
                    Math.min(numEntriesToScan.get() * 2, scanMaxBatchSize));
            // update start entry id
            curEndEntryId.set(Math.min(endEntryId, nextStartEntryId + numEntriesToScan.get() - 1));
            return true;
        }
    }

    /**
     * Read record from a given range of ledger entries.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param ledgerDescriptor
     *          ledger descriptor.
     * @param handleCache
     *          ledger handle cache.
     * @param executorService
     *          executor service used for processing entries
     * @param context
     *          scan context
     * @return a future with the log record.
     */
    private static Future<LogRecordWithDLSN> asyncReadRecordFromEntries(
            final String streamName,
            final LedgerDescriptor ledgerDescriptor,
            LedgerHandleCache handleCache,
            final LogSegmentMetadata metadata,
            final ExecutorService executorService,
            final ScanContext context,
            final LogRecordSelector selector) {
        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
        final long startEntryId = context.curStartEntryId.get();
        final long endEntryId = context.curEndEntryId.get();
        if (LOG.isDebugEnabled()) {
            LOG.debug("{} reading entries [{} - {}] from {}.",
                    new Object[] { streamName, startEntryId, endEntryId, ledgerDescriptor });
        }
        handleCache.asyncReadEntries(ledgerDescriptor, startEntryId, endEntryId,
                new org.apache.bookkeeper.client.AsyncCallback.ReadCallback() {
                    @Override
                    public void readComplete(final int rc, final LedgerHandle lh, final Enumeration<LedgerEntry> entries, Object ctx) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{} finished reading entries [{} - {}] from {} : {}",
                                    new Object[] { streamName, startEntryId, endEntryId, ledgerDescriptor, BKException.getMessage(rc) });
                        }
                        // submit the handling logic back to DL threads
                        executorService.submit(new Runnable() {
                            @Override
                            public void run() {
                                if (BKException.Code.OK != rc) {
                                    String errMsg = "Error reading entries [" + startEntryId + "-" + endEntryId
                                            + "] for reading record of " + streamName;
                                    promise.setException(new IOException(errMsg, BKException.create(rc)));
                                } else {
                                    LogRecordWithDLSN record = null;
                                    while (entries.hasMoreElements()) {
                                        LedgerEntry entry = entries.nextElement();
                                        try {
                                            visitEntryRecords(
                                                streamName, metadata, ledgerDescriptor.getLedgerSequenceNo(), entry, context, selector);
                                        } catch (IOException ioe) {
                                            // exception is only thrown due to bad ledger entry, so it might be corrupted
                                            // we shouldn't do anything beyond this point. throw the exception to application
                                            promise.setException(ioe);
                                            return;
                                        }
                                    }

                                    record = selector.result();
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("{} got record from entries [{} - {}] of {} : {}",
                                                new Object[] { streamName, startEntryId, endEntryId,
                                                        ledgerDescriptor, record });
                                    }
                                    promise.setValue(record);
                                }
                            }
                        });
                    }
                }, null);
        return promise;
    }

    /**
     * Process each record using LogRecordSelector.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param ledgerSeqNo
     *          ledger sequence number
     * @param entry
     *          ledger entry
     * @param context
     *          scan context
     * @return log record with dlsn inside the ledger entry
     * @throws IOException
     */
    private static void visitEntryRecords(
            String streamName,
            LogSegmentMetadata metadata,
            long ledgerSeqNo,
            LedgerEntry entry,
            ScanContext context,
            LogRecordSelector selector) throws IOException {
        LogRecord.Reader reader =
                new LedgerEntryReader(streamName, ledgerSeqNo, entry,
                        metadata.getEnvelopeEntries(), metadata.getStartSequenceId(),
                        NullStatsLogger.INSTANCE);
        LogRecordWithDLSN nextRecord = reader.readOp();
        while (nextRecord != null) {
            LogRecordWithDLSN record = nextRecord;
            nextRecord = reader.readOp();
            context.numRecordsScanned.incrementAndGet();
            if (!context.includeControl && record.isControl()) {
                continue;
            }
            if (!context.includeEndOfStream && record.isEndOfStream()) {
                continue;
            }
            selector.process(record);
        }
    }

    /**
     * Scan entries for the given record.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param ledgerDescriptor
     *          ledger descriptor.
     * @param handleCache
     *          ledger handle cache.
     * @param executorService
     *          executor service used for processing entries
     * @param promise
     *          promise to return desired record.
     * @param context
     *          scan context
     */
    private static void asyncReadRecordFromEntries(
            final String streamName,
            final LedgerDescriptor ledgerDescriptor,
            final LedgerHandleCache handleCache,
            final LogSegmentMetadata metadata,
            final ExecutorService executorService,
            final Promise<LogRecordWithDLSN> promise,
            final ScanContext context,
            final LogRecordSelector selector) {
        asyncReadRecordFromEntries(streamName, ledgerDescriptor, handleCache, metadata, executorService, context, selector)
                .addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                    @Override
                    public void onSuccess(LogRecordWithDLSN value) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{} read record from [{} - {}] of {} : {}",
                                    new Object[]{streamName, context.curStartEntryId.get(), context.curEndEntryId.get(),
                                            ledgerDescriptor, value});
                        }
                        if (null != value) {
                            promise.setValue(value);
                            return;
                        }
                        if (!context.moveToNextRange()) {
                            // no entries to read again
                            promise.setValue(null);
                            return;
                        }
                        // scan next range
                        asyncReadRecordFromEntries(streamName,
                                ledgerDescriptor,
                                handleCache,
                                metadata,
                                executorService,
                                promise,
                                context,
                                selector);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                });
    }

    private static void asyncReadRecordFromLedger(
            final String streamName,
            final LedgerDescriptor ledgerDescriptor,
            final LedgerHandleCache handleCache,
            final LogSegmentMetadata metadata,
            final ExecutorService executorService,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final Promise<LogRecordWithDLSN> promise,
            final AtomicInteger numRecordsScanned,
            final LogRecordSelector selector,
            final boolean backward,
            final long startEntryId) {
        final long lastAddConfirmed;
        try {
            lastAddConfirmed = handleCache.getLastAddConfirmed(ledgerDescriptor);
        } catch (IOException e) {
            promise.setException(e);
            return;
        }
        if (lastAddConfirmed < 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ledger {} is empty for {}.", new Object[] { ledgerDescriptor, streamName });
            }
            promise.setValue(null);
            return;
        }
        final ScanContext context = new ScanContext(
                startEntryId, lastAddConfirmed,
                scanStartBatchSize, scanMaxBatchSize,
                includeControl, includeEndOfStream, backward, numRecordsScanned);
        asyncReadRecordFromEntries(streamName, ledgerDescriptor, handleCache, metadata, executorService,
                                   promise, context, selector);
    }

    /**
     * Read last record from a ledger.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param l
     *          ledger descriptor.
     * @param fence
     *          whether to fence the ledger.
     * @param includeControl
     *          whether to include control record.
     * @param includeEndOfStream
     *          whether to include end of stream.
     * @param scanStartBatchSize
     *          first num entries used for read last record scan
     * @param scanMaxBatchSize
     *          max num entries used for read last record scan
     * @param numRecordsScanned
     *          num of records scanned to get last record
     * @param executorService
     *          executor service used for processing entries
     * @param bookKeeperClient
     *          bookkeeper client to process entries
     * @param digestpw
     *          digest password to read entries from bookkeeper
     * @return a future with last record.
     */
    public static Future<LogRecordWithDLSN> asyncReadLastRecord(
            final String streamName,
            final LogSegmentMetadata l,
            final boolean fence,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final BookKeeperClient bookKeeperClient,
            final String digestpw) {
        final LogRecordSelector selector = new LastRecordSelector();
        return asyncReadRecord(streamName, l, fence, includeControl, includeEndOfStream, scanStartBatchSize,
                               scanMaxBatchSize, numRecordsScanned, executorService, bookKeeperClient, digestpw,
                               selector, true /* backward */, 0L);
    }

    /**
     * Read first record from a ledger with a DLSN larger than that given.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param l
     *          ledger descriptor.
     * @param scanStartBatchSize
     *          first num entries used for read last record scan
     * @param scanMaxBatchSize
     *          max num entries used for read last record scan
     * @param numRecordsScanned
     *          num of records scanned to get last record
     * @param executorService
     *          executor service used for processing entries
     * @param bookKeeperClient
     *          bookkeeper client to process entries
     * @param digestpw
     *          digest password to read entries from bookkeeper
     * @param dlsn
     *          threshold dlsn
     * @return a future with last record.
     */
    static Future<LogRecordWithDLSN> asyncReadFirstUserRecord(
            final String streamName,
            final LogSegmentMetadata l,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final BookKeeperClient bookKeeperClient,
            final String digestpw,
            final DLSN dlsn) {
        long startEntryId = 0L;
        if (l.getLedgerSequenceNumber() == dlsn.getLedgerSequenceNo()) {
            startEntryId = dlsn.getEntryId();
        }
        final LogRecordSelector selector = new FirstDLSNGreaterThanSelector(dlsn);
        return asyncReadRecord(streamName, l, false, false, false, scanStartBatchSize,
                               scanMaxBatchSize, numRecordsScanned, executorService, bookKeeperClient, digestpw,
                               selector, false /* backward */, startEntryId);
    }

    private static Future<LogRecordWithDLSN> asyncReadRecord(
            final String streamName,
            final LogSegmentMetadata l,
            final boolean fence,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final BookKeeperClient bookKeeperClient,
            final String digestpw,
            final LogRecordSelector selector,
            final boolean backward,
            final long startEntryId) {

        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
        // Create a ledger handle cache && open ledger handle
        final LedgerHandleCache handleCachePriv = new LedgerHandleCache(bookKeeperClient, digestpw);
        // close the ledger handle cache once the promise is satisified
        promise.ensure(new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                handleCachePriv.clear();
                return BoxedUnit.UNIT;
            }
        });

        handleCachePriv.asyncOpenLedger(l, fence, new BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>() {
            @Override
            public void operationComplete(final int rc, final LedgerDescriptor ledgerDescriptor) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} Opened logsegment {} for reading record : {}",
                            new Object[] { streamName, l, BKException.getMessage(rc) });
                }
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            String errMsg = "Error opening log segment " + l + "] for reading record of " + streamName;
                            promise.setException(new IOException(errMsg, BKException.create(rc)));
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("{} {} scanning {}.", new Object[] {
                                        (backward ? "backward" : "forward"), streamName, l });
                            }
                            asyncReadRecordFromLedger(
                                    streamName, ledgerDescriptor, handleCachePriv, l, executorService,
                                    scanStartBatchSize, scanMaxBatchSize,
                                    includeControl, includeEndOfStream,
                                    promise, numRecordsScanned, selector, backward, startEntryId);
                        }
                    }
                });
            }
        });
        return promise;
    }
}
