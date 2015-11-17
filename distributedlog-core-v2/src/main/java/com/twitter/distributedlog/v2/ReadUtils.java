package com.twitter.distributedlog.v2;

import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.v2.selector.FirstTxIdNotLessThanSelector;
import com.twitter.distributedlog.v2.selector.LogRecordSelector;
import com.twitter.distributedlog.v2.util.FutureUtils;
import com.twitter.distributedlog.v2.util.FutureUtils.FutureEventListenerRunnable;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import static com.twitter.distributedlog.DLSNUtil.*;

/**
 * Utility function for readers
 */
public class ReadUtils {

    static final Logger LOG = LoggerFactory.getLogger(ReadUtils.class);

    //
    // Private methods for scanning log segments
    //

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

    private static class SingleEntryScanContext extends ScanContext {
        SingleEntryScanContext(long entryId) {
            super(entryId, entryId, 1, 1, true, true, false, new AtomicInteger(0));
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
            final LogSegmentLedgerMetadata metadata,
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
        final FutureEventListener<Enumeration<LedgerEntry>> readEntriesListener = FutureEventListenerRunnable.of(
            new FutureEventListener<Enumeration<LedgerEntry>>() {
                @Override
                public void onSuccess(final Enumeration<LedgerEntry> entries) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} finished reading entries [{} - {}] from {}",
                                new Object[]{ streamName, startEntryId, endEntryId, ledgerDescriptor });
                    }
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
                                new Object[]{streamName, startEntryId, endEntryId,
                                        ledgerDescriptor, record});
                    }
                    promise.setValue(record);
                }

                @Override
                public void onFailure(final Throwable cause) {
                    String errMsg = "Error reading entries [" + startEntryId + "-" + endEntryId
                                + "] for reading record of " + streamName;
                    promise.setException(new IOException(errMsg,
                            BKException.create(FutureUtils.bkResultCode(cause))));
                }
            }, executorService);
        handleCache.asyncReadEntries(ledgerDescriptor, startEntryId, endEntryId, new AsyncCallback.ReadCallback() {
            @Override
            public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> entries, Object ctx) {
                if (BKException.Code.OK == rc) {
                    readEntriesListener.onSuccess(entries);
                } else {
                    readEntriesListener.onFailure(BKException.create(rc));
                }
            }
        }, null);
        return promise;
    }

    /**
     * Process each record using LogRecordSelector.
     *
     * @param streamName
     *          fully qualified stream name (used for logging)
     * @param logSegmentSeqNo
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
            LogSegmentLedgerMetadata metadata,
            long logSegmentSeqNo,
            LedgerEntry entry,
            ScanContext context,
            LogRecordSelector selector) throws IOException {
        LogRecord.Reader reader =
                new LedgerEntryReader(streamName, logSegmentSeqNo, entry,
                        metadata.getEnvelopeEntries(), NullStatsLogger.INSTANCE);
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

    //
    // Search Functions
    //

    /**
     * Get the log record whose transaction id is not less than provided <code>transactionId</code>.
     *
     * <p>
     * It uses a binary-search like algorithm to find the log record whose transaction id is not less than
     * provided <code>transactionId</code> within a log <code>segment</code>. You could think of a log segment
     * in terms of a sequence of records whose transaction ids are non-decreasing.
     *
     * - The sequence of records within a log segment is divided into N pieces.
     * - Find the piece of records that contains a record whose transaction id is not less than provided
     *   <code>transactionId</code>.
     *
     * N could be chosen based on trading off concurrency and latency.
     * </p>
     *
     * @param logName
     *          name of the log
     * @param segment
     *          metadata of the log segment
     * @param transactionId
     *          transaction id
     * @param executorService
     *          executor service used for processing entries
     * @param handleCache
     *          ledger handle cache
     * @param nWays
     *          how many number of entries to search in parallel
     * @return found log record. none if all transaction ids are less than provided <code>transactionId</code>.
     */
    public static Future<Optional<LogRecordWithDLSN>> getLogRecordNotLessThanTxId(
            final String logName,
            final LogSegmentLedgerMetadata segment,
            final long transactionId,
            final ExecutorService executorService,
            final LedgerHandleCache handleCache,
            final int nWays) {
        if (!segment.isInProgress()) {
            if (segment.getLastTxId() < transactionId) {
                // all log records whose transaction id is less than provided transactionId
                // then return none
                Optional<LogRecordWithDLSN> noneRecord = Optional.absent();
                return Future.value(noneRecord);
            }
        }

        final Promise<Optional<LogRecordWithDLSN>> promise =
                new Promise<Optional<LogRecordWithDLSN>>();
        final FutureEventListener<LedgerDescriptor> openLedgerListener = FutureEventListenerRunnable.of(
            new FutureEventListener<LedgerDescriptor>() {
                @Override
                public void onSuccess(final LedgerDescriptor ld) {
                    promise.ensure(new AbstractFunction0<BoxedUnit>() {
                        @Override
                        public BoxedUnit apply() {
                            try {
                                handleCache.closeLedger(ld);
                            } catch (InterruptedException e) {
                                // no-op for readonly ledger handle
                            } catch (BKException e) {
                                // no-op for readonly ledger handle
                            }
                            return BoxedUnit.UNIT;
                        }
                    });
                    // all log records whose transaction id is not less than provided transactionId
                    if (segment.getFirstTxId() >= transactionId) {
                        final FirstTxIdNotLessThanSelector selector =
                                new FirstTxIdNotLessThanSelector(transactionId);
                        asyncReadRecordFromEntries(
                                logName,
                                ld,
                                handleCache,
                                segment,
                                executorService,
                                new SingleEntryScanContext(0L),
                                selector
                        ).addEventListener(new FutureEventListener<LogRecordWithDLSN>() {
                            @Override
                            public void onSuccess(LogRecordWithDLSN value) {
                                promise.setValue(Optional.of(selector.result()));
                            }

                            @Override
                            public void onFailure(Throwable cause) {
                                promise.setException(cause);
                            }
                        });

                        return;
                    }
                    long lastEntryId;
                    try {
                        lastEntryId = handleCache.getLastAddConfirmed(ld);
                    } catch (IOException e) {
                        promise.setException(e);
                        return;
                    }
                    getLogRecordNotLessThanTxIdFromEntries(
                            logName,
                            ld,
                            segment,
                            transactionId,
                            executorService,
                            handleCache,
                            Lists.newArrayList(0L, lastEntryId),
                            nWays,
                            Optional.<LogRecordWithDLSN>absent(),
                            promise);
                }

                @Override
                public void onFailure(final Throwable cause) {
                    String errMsg = "Error opening log segment [" + segment
                            + "] for find record from " + logName;
                    promise.setException(new IOException(errMsg,
                            BKException.create(FutureUtils.bkResultCode(cause))));
                }
            }, executorService);
        handleCache.asyncOpenLedger(segment, false, new BookkeeperInternalCallbacks.GenericCallback<LedgerDescriptor>() {
            @Override
            public void operationComplete(int rc, LedgerDescriptor ledgerDescriptor) {
                if (BKException.Code.OK == rc) {
                    openLedgerListener.onSuccess(ledgerDescriptor);
                } else {
                    openLedgerListener.onFailure(BKException.create(rc));
                }
            }
        });
        return promise;
    }

    /**
     * Find the log record whose transaction id is not less than provided <code>transactionId</code> from
     * entries between <code>startEntryId</code> and <code>endEntryId</code>.
     *
     * @param logName
     *          name of the log
     * @param segment
     *          log segment
     * @param transactionId
     *          provided transaction id to search
     * @param executorService
     *          executor service
     * @param handleCache
     *          handle cache
     * @param entriesToSearch
     *          list of entries to search
     * @param nWays
     *          how many entries to search in parallel
     * @param prevFoundRecord
     *          the log record found in previous search
     * @param promise
     *          promise to satisfy the result
     */
    private static void getLogRecordNotLessThanTxIdFromEntries(
            final String logName,
            final LedgerDescriptor ld,
            final LogSegmentLedgerMetadata segment,
            final long transactionId,
            final ExecutorService executorService,
            final LedgerHandleCache handleCache,
            final List<Long> entriesToSearch,
            final int nWays,
            final Optional<LogRecordWithDLSN> prevFoundRecord,
            final Promise<Optional<LogRecordWithDLSN>> promise) {
        final List<Future<LogRecordWithDLSN>> searchResults =
                Lists.newArrayListWithExpectedSize(entriesToSearch.size());
        for (Long entryId : entriesToSearch) {
            LogRecordSelector selector = new FirstTxIdNotLessThanSelector(transactionId);
            Future<LogRecordWithDLSN> searchResult = asyncReadRecordFromEntries(
                    logName,
                    ld,
                    handleCache,
                    segment,
                    executorService,
                    new SingleEntryScanContext(entryId),
                    selector);
            searchResults.add(searchResult);
        }
        FutureEventListener<List<LogRecordWithDLSN>> processSearchResultsListener =
                new FutureEventListener<List<LogRecordWithDLSN>>() {
                    @Override
                    public void onSuccess(List<LogRecordWithDLSN> resultList) {
                        processSearchResults(
                                logName,
                                ld,
                                segment,
                                transactionId,
                                executorService,
                                handleCache,
                                resultList,
                                nWays,
                                prevFoundRecord,
                                promise);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        promise.setException(cause);
                    }
                };
        Future.collect(searchResults).addEventListener(
                FutureEventListenerRunnable.of(processSearchResultsListener, executorService));
    }

    /**
     * Process the search results
     */
    static void processSearchResults(
            final String logName,
            final LedgerDescriptor ld,
            final LogSegmentLedgerMetadata segment,
            final long transactionId,
            final ExecutorService executorService,
            final LedgerHandleCache handleCache,
            final List<LogRecordWithDLSN> searchResults,
            final int nWays,
            final Optional<LogRecordWithDLSN> prevFoundRecord,
            final Promise<Optional<LogRecordWithDLSN>> promise) {
        int found = -1;
        for (int i = 0; i < searchResults.size(); i++) {
            LogRecordWithDLSN record = searchResults.get(i);
            if (record.getTransactionId() >= transactionId) {
                found = i;
                break;
            }
        }
        if (found == -1) { // all log records' transaction id is less than provided transaction id
            promise.setValue(prevFoundRecord);
            return;
        }
        // we found a log record
        LogRecordWithDLSN foundRecord = searchResults.get(found);

        // we found it
        //   - it is not the first record
        //   - it is the first record in first search entry
        //   - its entry is adjacent to previous search entry
        if (getSlotId(foundRecord.getDlsn()) != 0L
                || found == 0
                || getEntryId(foundRecord.getDlsn()) == (getEntryId(searchResults.get(found - 1).getDlsn()) + 1)) {
            promise.setValue(Optional.of(foundRecord));
            return;
        }

        // otherwise, we need to search
        List<Long> nextSearchBatch = getEntriesToSearch(
                transactionId,
                searchResults.get(found - 1),
                searchResults.get(found),
                nWays);
        if (nextSearchBatch.isEmpty()) {
            promise.setValue(prevFoundRecord);
            return;
        }
        getLogRecordNotLessThanTxIdFromEntries(
                logName,
                ld,
                segment,
                transactionId,
                executorService,
                handleCache,
                nextSearchBatch,
                nWays,
                Optional.of(foundRecord),
                promise);
    }

    /**
     * Get the entries to search provided <code>transactionId</code> between
     * <code>firstRecord</code> and <code>lastRecord</code>. <code>firstRecord</code>
     * and <code>lastRecord</code> are already searched, which the transaction id
     * of <code>firstRecord</code> is less than <code>transactionId</code> and the
     * transaction id of <code>lastRecord</code> is not less than <code>transactionId</code>.
     *
     * @param transactionId
     *          transaction id to search
     * @param firstRecord
     *          log record that already searched whose transaction id is leass than <code>transactionId</code>.
     * @param lastRecord
     *          log record that already searched whose transaction id is not less than <code>transactionId</code>.
     * @param nWays
     *          N-ways to search
     * @return the list of entries to search
     */
    static List<Long> getEntriesToSearch(
            long transactionId,
            LogRecordWithDLSN firstRecord,
            LogRecordWithDLSN lastRecord,
            int nWays) {
        long txnDiff = lastRecord.getTransactionId() - firstRecord.getTransactionId();
        if (txnDiff > 0) {
            if (lastRecord.getTransactionId() == transactionId) {
                List<Long> entries = getEntriesToSearch(
                        getEntryId(firstRecord.getDlsn()) + 1,
                        getEntryId(lastRecord.getDlsn()) - 2,
                        Math.max(1, nWays - 1));
                entries.add(getEntryId(lastRecord.getDlsn()) - 1);
                return entries;
            } else {
                // TODO: improve it by estimating transaction ids.
                return getEntriesToSearch(
                        getEntryId(firstRecord.getDlsn()) + 1,
                        getEntryId(lastRecord.getDlsn()) - 1,
                        nWays);
            }
        } else {
            // unexpected condition
            return Lists.newArrayList();
        }
    }

    static List<Long> getEntriesToSearch(
            long startEntryId,
            long endEntryId,
            int nWays) {
        if (startEntryId > endEntryId) {
            return Lists.newArrayList();
        }
        long numEntries = endEntryId - startEntryId + 1;
        long step = Math.max(1L, numEntries / nWays);
        List<Long> entryList = Lists.newArrayListWithExpectedSize(nWays);
        for (long i = startEntryId, j = nWays - 1; i <= endEntryId && j > 0; i+=step, j--) {
            entryList.add(i);
        }
        if (entryList.get(entryList.size() - 1) < endEntryId) {
            entryList.add(endEntryId);
        }
        return entryList;
    }
}
