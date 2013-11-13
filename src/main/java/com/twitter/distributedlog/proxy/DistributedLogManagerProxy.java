package com.twitter.distributedlog.proxy;

import com.twitter.distributedlog.AsyncLogWriter;

public interface DistributedLogManagerProxy {

    AsyncLogWriter getAsyncLogWriter(String streamName);
}
