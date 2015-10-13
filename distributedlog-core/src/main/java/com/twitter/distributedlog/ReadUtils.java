package com.twitter.distributedlog;

import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.selector.FirstDLSNNotLessThanSelector;
import com.twitter.distributedlog.selector.FirstTxIdNotLessThanSelector;
import com.twitter.distributedlog.selector.LastRecordSelector;
import com.twitter.distributedlog.selector.LogRecordSelector;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.FutureUtils.FutureEventListenerRunnable;
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

    //
    // Read First & Last Record Functions
    //

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
     * @param handleCache
     *          ledger handle cache
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
            final LedgerHandleCache handleCache) {
        final LogRecordSelector selector = new LastRecordSelector();
        return asyncReadRecord(streamName, l, fence, includeControl, includeEndOfStream, scanStartBatchSize,
                               scanMaxBatchSize, numRecordsScanned, executorService, handleCache,
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
     * @param dlsn
     *          threshold dlsn
     * @return a future with last record.
     */
    public static Future<LogRecordWithDLSN> asyncReadFirstUserRecord(
            final String streamName,
            final LogSegmentMetadata l,
            final int scanStartBatchSize,
            final int scanMaxBatchSize,
            final AtomicInteger numRecordsScanned,
            final ExecutorService executorService,
            final LedgerHandleCache handleCache,
            final DLSN dlsn) {
        long startEntryId = 0L;
        if (l.getLogSegmentSequenceNumber() == dlsn.getLogSegmentSequenceNo()) {
            startEntryId = dlsn.getEntryId();
        }
        final LogRecordSelector selector = new FirstDLSNNotLessThanSelector(dlsn);
        return asyncReadRecord(streamName, l, false, false, false, scanStartBatchSize,
                               scanMaxBatchSize, numRecordsScanned, executorService, handleCache,
                               selector, false /* backward */, startEntryId);
    }

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
        FutureEventListener<Enumeration<LedgerEntry>> readEntriesListener =
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
                                    streamName, metadata, ledgerDescriptor.getLogSegmentSequenceNo(), entry, context, selector);
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
            };
        handleCache.asyncReadEntries(ledgerDescriptor, startEntryId, endEntryId)
                .addEventListener(FutureEventListenerRunnable.of(readEntriesListener, executorService));
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
            LogSegmentMetadata metadata,
            long logSegmentSeqNo,
            LedgerEntry entry,
            ScanContext context,
            LogRecordSelector selector) throws IOException {
        LogRecord.Reader reader =
                new LedgerEntryReader(streamName, logSegmentSeqNo, entry,
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
        FutureEventListener<LogRecordWithDLSN> readEntriesListener =
            new FutureEventListener<LogRecordWithDLSN>() {
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
            };
        asyncReadRecordFromEntries(streamName, ledgerDescriptor, handleCache, metadata, executorService, context, selector)
                .addEventListener(FutureEventListenerRunnable.of(readEntriesListener, executorService));
    }

    private static void asyncReadRecordFromLogSegment(
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
        } catch (BKException e) {
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
            final LedgerHandleCache handleCache,
            final LogRecordSelector selector,
            final boolean backward,
            final long startEntryId) {

        final Promise<LogRecordWithDLSN> promise = new Promise<LogRecordWithDLSN>();

        FutureEventListener<LedgerDescriptor> openLedgerListener =
            new FutureEventListener<LedgerDescriptor>() {
                @Override
                public void onSuccess(final LedgerDescriptor ledgerDescriptor) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} Opened logsegment {} for reading record",
                                streamName, l);
                    }
                    promise.ensure(new AbstractFunction0<BoxedUnit>() {
                        @Override
                        public BoxedUnit apply() {
                            handleCache.asyncCloseLedger(ledgerDescriptor);
                            return BoxedUnit.UNIT;
                        }
                    });
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} {} scanning {}.", new Object[]{
                                (backward ? "backward" : "forward"), streamName, l});
                    }
                    asyncReadRecordFromLogSegment(
                            streamName, ledgerDescriptor, handleCache, l, executorService,
                            scanStartBatchSize, scanMaxBatchSize,
                            includeControl, includeEndOfStream,
                            promise, numRecordsScanned, selector, backward, startEntryId);
                }

                @Override
                public void onFailure(final Throwable cause) {
                    String errMsg = "Error opening log segment [" + l + "] for reading record of " + streamName;
                    promise.setException(new IOException(errMsg,
                            BKException.create(FutureUtils.bkResultCode(cause))));
                }
            };
        handleCache.asyncOpenLedger(l, fence)
                .addEventListener(FutureEventListenerRunnable.of(openLedgerListener, executorService));
        return promise;
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
            final LogSegmentMetadata segment,
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
        final FutureEventListener<LedgerDescriptor> openLedgerListener =
            new FutureEventListener<LedgerDescriptor>() {
                @Override
                public void onSuccess(final LedgerDescriptor ld) {
                    promise.ensure(new AbstractFunction0<BoxedUnit>() {
                        @Override
                        public BoxedUnit apply() {
                            handleCache.asyncCloseLedger(ld);
                            return BoxedUnit.UNIT;
                        }

                    });
                    long lastEntryId;
                    try {
                        lastEntryId = handleCache.getLastAddConfirmed(ld);
                    } catch (BKException e) {
                        promise.setException(e);
                        return;
                    }
                    if (lastEntryId < 0) {
                        // it means that the log segment is created but not written yet or an empty log segment.
                        // it is equivalent to 'all log records whose transaction id is less than provided transactionId'
                        Optional<LogRecordWithDLSN> nonRecord = Optional.absent();
                        promise.setValue(nonRecord);
                        return;
                    }
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
            };
        handleCache.asyncOpenLedger(segment, false)
                .addEventListener(FutureEventListenerRunnable.of(openLedgerListener, executorService));
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
            final LogSegmentMetadata segment,
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
            final LogSegmentMetadata segment,
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
        if (foundRecord.getDlsn().getSlotId() != 0L
                || found == 0
                || foundRecord.getDlsn().getEntryId() == (searchResults.get(found - 1).getDlsn().getEntryId() + 1)) {
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
                        firstRecord.getDlsn().getEntryId() + 1,
                        lastRecord.getDlsn().getEntryId() - 2,
                        Math.max(1, nWays - 1));
                entries.add(lastRecord.getDlsn().getEntryId() - 1);
                return entries;
            } else {
                // TODO: improve it by estimating transaction ids.
                return getEntriesToSearch(
                        firstRecord.getDlsn().getEntryId() + 1,
                        lastRecord.getDlsn().getEntryId() - 1,
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
