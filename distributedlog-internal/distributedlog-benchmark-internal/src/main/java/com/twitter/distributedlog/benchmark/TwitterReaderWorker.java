package com.twitter.distributedlog.benchmark;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.distributedlog.service.TwitterServerSetUtils;
import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.StatsLogger;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Twitter Reader Worker to handle serversets
 */
class TwitterReaderWorker extends ReaderWorker {

    TwitterReaderWorker(DistributedLogConfiguration conf,
                        URI uri,
                        String streamPrefix,
                        int startStreamId,
                        int endStreamId,
                        int readThreadPoolSize,
                        List<String> serverSetPaths,
                        List<String> finagleNames,
                        int truncationIntervalInSeconds,
                        boolean readFromHead,
                        StatsReceiver statsReceiver,
                        StatsLogger statsLogger)
            throws IOException {
        super(conf,
                uri,
                streamPrefix,
                startStreamId,
                endStreamId,
                readThreadPoolSize,
                serverSetPaths,
                finagleNames,
                truncationIntervalInSeconds,
                readFromHead,
                statsReceiver,
                statsLogger);
    }

    @Override
    protected DLZkServerSet[] createServerSets(List<String> serverSetPaths) {
        DLZkServerSet[] serverSets = new DLZkServerSet[serverSetPaths.size()];
        for (int i = 0; i < serverSets.length; i++) {
            String serverSetPath = serverSetPaths.get(i);
            serverSets[i] = TwitterServerSetUtils.parseServerSet(serverSetPath);
        }
        return serverSets;
    }
}
