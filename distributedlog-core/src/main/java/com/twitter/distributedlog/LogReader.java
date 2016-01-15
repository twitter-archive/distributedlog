package com.twitter.distributedlog;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * <i>LogReader</i> is a `synchronous` reader reading records from a DL log.
 *
 * <h3>Lifecycle of a Reader</h3>
 *
 * A reader is a <i>sequential</i> reader that read records from a DL log starting
 * from a given position. The position could be a <i>DLSN</i> (via {@link DistributedLogManager#getInputStream(DLSN)}
 * or a <i>Transaction ID</i> (via {@link DistributedLogManager#getInputStream(long)}.
 * <p>
 * After the reader is open, it could call {@link #readNext(boolean)} or {@link #readBulk(boolean, int)}
 * to read records out the log from provided position.
 * <p>
 * Closing the reader (via {@link #close()} will release all the resources occupied
 * by this reader instance.
 * <p>
 * Exceptions could be thrown during reading records. Once the exception is thrown,
 * the reader is set to an error state and it isn't usable anymore. It is the application's
 * responsibility to handle the exceptions and re-create readers if necessary.
 * <p>
 * Example:
 * <pre>
 * DistributedLogManager dlm = ...;
 * long nextTxId = ...;
 * LogReader reader = dlm.getInputStream(nextTxId);
 *
 * while (true) { // keep reading & processing records
 *     LogRecord record;
 *     try {
 *         record = reader.readNext(false);
 *         nextTxId = record.getTransactionId();
 *         // process the record
 *         ...
 *     } catch (IOException ioe) {
 *         // handle the exception
 *         ...
 *         reader = dlm.getInputStream(nextTxId + 1);
 *     }
 * }
 *
 * </pre>
 *
 * <h3>Read Records</h3>
 *
 * Reading records from an <i>endless</i> log in `synchronous` way isn't as
 * trivial as in `asynchronous` way (via {@link AsyncLogReader}. Because it
 * lacks of callback mechanism. LogReader introduces a flag `nonBlocking` on
 * controlling the <i>waiting</i> behavior on `synchronous` reads.
 *
 * <h4>Blocking vs NonBlocking</h4>
 *
 * <i>Blocking</i> (nonBlocking = false) means the reads will wait for records
 * before returning read calls. While <i>NonBlocking</i> (nonBlocking = true)
 * means the reads will only check readahead cache and return whatever records
 * available in the readahead cache.
 * <p>
 * The <i>waiting</i> period varies in <i>blocking</i> mode. If the reader is
 * catching up with writer (there are records in the log), the read call will
 * wait until records are read and returned. If the reader is caught up with
 * writer (there are no more records in the log at read time), the read call
 * will wait for a small period of time (defined in
 * {@link DistributedLogConfiguration#getReadAheadWaitTime()} and return whatever
 * records available in the readahead cache. In other words, if a reader sees
 * no record on blocking reads, it means the reader is `caught-up` with the
 * writer.
 * <p>
 * <i>Blocking</i> and <i>NonBlocking</i> modes are useful for building replicated
 * state machines. Applications could use <i>blocking</i> reads till caught up
 * with latest data. Once they are caught up with latest data, they could start
 * serving their service and turn to <i>non-blocking</i> read mode and tail read
 * data from the logs.
 * <p>
 * See examples below.
 *
 * <h4>Read Single Record</h4>
 *
 * {@link #readNext(boolean)} is reading individual records from a DL log.
 *
 * <pre>
 * LogReader reader = ...
 *
 * // keep reading records in blocking way until no records available in the log
 * LogRecord record = reader.readNext(false);
 * while (null != record) {
 *     // process the record
 *     ...
 *     // read next record
 *     records = reader.readNext(false);
 * }
 *
 * ...
 *
 * // reader is caught up with writer, doing non-blocking reads to tail the log
 * while (true) {
 *     record = reader.readNext(true)
 *     // process the new records
 *     ...
 * }
 * </pre>
 *
 * <h4>Read Batch of Records</h4>
 *
 * {@link #readBulk(boolean, int)} is a convenient way to read a batch of records
 * from a DL log.
 *
 * <pre>
 * LogReader reader = ...
 * int N = 10;
 *
 * // keep reading N records in blocking way until no records available in the log
 * List<LogRecord> records = reader.readBulk(false, N);
 * while (!records.isEmpty()) {
 *     // process the list of records
 *     ...
 *     if (records.size() < N) { // no more records available in the log
 *         break;
 *     }
 *     // read next N records
 *     records = reader.readBulk(false, N);
 * }
 *
 * ...
 *
 * // reader is caught up with writer, doing non-blocking reads to tail the log
 * while (true) {
 *     records = reader.readBulk(true, N)
 *     // process the new records
 *     ...
 * }
 *
 * </pre>
 *
 * @see AsyncLogReader
 */
public interface LogReader extends Closeable {

    /**
     * Close the stream.
     *
     * @throws IOException if an error occurred while closing
     */
    public void close() throws IOException;

    /**
     * Read the next log record from the stream.
     * <p>
     * If <i>nonBlocking</i> is set to true, the call returns immediately by just polling
     * records from read ahead cache. It would return <i>null</i> if there isn't any records
     * available in the read ahead cache.
     * <p>
     * If <i>nonBlocking</i> is set to false, it would does blocking call. The call will
     * block until return a record if there are records in the stream (aka catching up).
     * Otherwise it would wait up to {@link DistributedLogConfiguration#getReadAheadWaitTime()}
     * milliseconds and return null if there isn't any more records in the stream.
     *
     * @param nonBlocking should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @return an operation from the stream or null if at end of stream
     * @throws IOException if there is an error reading from the stream
     */
    public LogRecordWithDLSN readNext(boolean nonBlocking) throws IOException;

    /**
     * Read the next <i>numLogRecords</i> log records from the stream
     *
     * @param nonBlocking should the read make blocking calls to the backend or rely on the
     * readAhead cache
     * @param numLogRecords maximum number of log records returned by this call.
     * @return an operation from the stream or empty list if at end of stream
     * @throws IOException if there is an error reading from the stream
     * @see #readNext(boolean)
     */
    public List<LogRecordWithDLSN> readBulk(boolean nonBlocking, int numLogRecords) throws IOException;
}
