package com.twitter.distributedlog;

import com.twitter.util.Future;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface AsyncLogReader extends Closeable {
    /**
     *
     * @param timeout - timeout value
     * @param timeUnit - units associated with the timeout value
     * @return A promise that when satisfied will contain the Log Record with its DLSN;
     * The Future may timeout if there is no record to return within the specified timeout
     * @throws IOException
     */
    public Future<LogRecordWithDLSN> readNext();
}
