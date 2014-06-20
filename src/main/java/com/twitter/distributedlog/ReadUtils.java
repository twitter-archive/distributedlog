package com.twitter.distributedlog;

import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        final int firstNumEntriesPerReadLastRecordScan;
        final int maxNumEntriesPerReadLastRecordScan;
        final boolean includeControl;
        final boolean includeEndOfStream;
        final boolean backward;

        // number of records scanned
        final AtomicInteger numRecordsScanned;

        ScanContext(long startEntryId, long endEntryId,
                    int firstNumEntriesPerReadLastRecordScan,
                    int maxNumEntriesPerReadLastRecordScan,
                    boolean includeControl,
                    boolean includeEndOfStream,
                    boolean backward,
                    AtomicInteger numRecordsScanned) {
            this.startEntryId = startEntryId;
            this.endEntryId = endEntryId;
            this.firstNumEntriesPerReadLastRecordScan = firstNumEntriesPerReadLastRecordScan;
            this.maxNumEntriesPerReadLastRecordScan = maxNumEntriesPerReadLastRecordScan;
            this.includeControl = includeControl;
            this.includeEndOfStream = includeEndOfStream;
            this.backward = backward;
            // Scan state
            this.numEntriesToScan = new AtomicInteger(firstNumEntriesPerReadLastRecordScan);
            if (backward) {
                this.curStartEntryId = new AtomicLong(
                        Math.max(startEntryId, (endEntryId - firstNumEntriesPerReadLastRecordScan + 1)));
                this.curEndEntryId = new AtomicLong(endEntryId);
            } else {
                this.curStartEntryId = new AtomicLong(startEntryId);
                this.curEndEntryId = new AtomicLong(
                        Math.min(endEntryId, (startEntryId + firstNumEntriesPerReadLastRecordScan - 1)));
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
                    Math.min(numEntriesToScan.get() * 2, maxNumEntriesPerReadLastRecordScan));
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
                    Math.min(numEntriesToScan.get() * 2, maxNumEntriesPerReadLastRecordScan));
            // update start entry id
            curEndEntryId.set(Math.min(endEntryId, nextStartEntryId + numEntriesToScan.get() - 1));
            return true;
        }
    }

    /**
     * Read last record from a given range of ledger entries.
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
     * @return a future with last log record.
     */
    private static Future<LogRecordWithDLSN> asyncReadLastRecordFromEntries(
            final String streamName,
            final LedgerDescriptor ledgerDescriptor,
            LedgerHandleCache handleCache,
            final ExecutorService executorService,
            final ScanContext context) {
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
                                            + "] for reading last record of " + streamName;
                                    promise.setException(new IOException(errMsg, BKException.create(rc)));
                                } else {
                                    // TODO: (sijieg) properly we need a reverse enumeration
                                    LogRecordWithDLSN lastRecord = null;
                                    while (entries.hasMoreElements()) {
                                        LedgerEntry entry = entries.nextElement();
                                        try {
                                            LogRecordWithDLSN record = getLastRecordFromEntry(
                                                    streamName, ledgerDescriptor.getLedgerSequenceNo(), entry, context);
                                            if (null != record) {
                                                lastRecord = record;
                                            }
                                        } catch (IOException ioe) {
                                            // exception is only thrown due to bad ledger entry, so it might be corrupted
                                            // we shouldn't do anything beyond this point. throw the exception to application
                                            promise.setException(ioe);
                                            return;
                                        }
                                    }
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("{} got last record from entries [{} - {}] of {} : {}",
                                                new Object[] { streamName, startEntryId, endEntryId,
                                                        ledgerDescriptor, lastRecord });
                                    }
                                    promise.setValue(lastRecord);
                                }
                            }
                        });
                    }
                }, null);
        return promise;
    }

    /**
     * Get last record from a given ledger entry. (it just processed the in-memory ledger entry).
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param ledgerSeqNo
     *          ledger sequence number
     * @param entry
     *          ledger entry
     * @param context
     *          scan context
     * @return last log record with dlsn inside the ledger entry
     * @throws IOException
     */
    private static LogRecordWithDLSN getLastRecordFromEntry(
            String streamName,
            long ledgerSeqNo,
            LedgerEntry entry,
            ScanContext context) throws IOException {
        LogRecord.Reader reader = new LedgerEntryReader(streamName, ledgerSeqNo, entry);
        LogRecordWithDLSN lastRecord = null;
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
            lastRecord = record;
        }
        return lastRecord;
    }

    /**
     * Do a backward scanning in given steps.
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
     *          promise to return last record.
     * @param context
     *          scan context
     */
    private static void asyncReadLastRecordFromEntries(
            final String streamName,
            final LedgerDescriptor ledgerDescriptor,
            final LedgerHandleCache handleCache,
            final ExecutorService executorService,
            final Promise<LogRecordWithDLSN> promise,
            final ScanContext context) {
        asyncReadLastRecordFromEntries(streamName, ledgerDescriptor, handleCache, executorService, context)
                .addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                    @Override
                    public void onSuccess(LogRecordWithDLSN value) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("{} read last record from [{} - {}] of {} : {}",
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
                        asyncReadLastRecordFromEntries(streamName,
                                ledgerDescriptor,
                                handleCache,
                                executorService,
                                promise,
                                context);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                });
    }

    private static void asyncReadLastRecordFromLedger(
            final String streamName,
            final LedgerDescriptor ledgerDescriptor,
            final LedgerHandleCache handleCache,
            final ExecutorService executorService,
            final int firstNumEntriesPerReadLastRecordScan,
            final int maxNumEntriesPerReadLastRecordScan,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final Promise<LogRecordWithDLSN> promise,
            final AtomicInteger numRecordsScanned) {
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
                0L, lastAddConfirmed,
                firstNumEntriesPerReadLastRecordScan, maxNumEntriesPerReadLastRecordScan,
                includeControl, includeEndOfStream, true, numRecordsScanned);
        asyncReadLastRecordFromEntries(streamName, ledgerDescriptor, handleCache, executorService,
                                       promise, context);
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
     * @param firstNumEntriesPerReadLastRecordScan
     *          first num entries used for read last record scan
     * @param maxNumEntriesPerReadLastRecordScan
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
            final LogSegmentLedgerMetadata l,
            final boolean fence,
            final boolean includeControl,
            final boolean includeEndOfStream,
            final int firstNumEntriesPerReadLastRecordScan,
            final int maxNumEntriesPerReadLastRecordScan,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final BookKeeperClient bookKeeperClient,
            final String digestpw) {
        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();
        // Create a ledger handle cache && open ledger handle
        final LedgerHandleCache handleCachePriv = new LedgerHandleCache(bookKeeperClient, digestpw);
        handleCachePriv.asyncOpenLedger(l, fence, new BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>() {
            @Override
            public void operationComplete(final int rc, final LedgerDescriptor ledgerDescriptor) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} Opened logsegment {} for reading last record : {}",
                            new Object[] { streamName, l, BKException.getMessage(rc) });
                }
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (BKException.Code.OK != rc) {
                            String errMsg = "Error opening log segment " + l
                                    + "] for reading last record of " + streamName;
                            promise.setException(new IOException(errMsg, BKException.create(rc)));
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("{} backward scanning {}.", streamName, l);
                            }
                            asyncReadLastRecordFromLedger(
                                    streamName, ledgerDescriptor, handleCachePriv, executorService,
                                    firstNumEntriesPerReadLastRecordScan, maxNumEntriesPerReadLastRecordScan,
                                    includeControl, includeEndOfStream,
                                    promise, numRecordsScanned);
                        }
                    }
                });
            }
        });
        return promise;
    }
}
