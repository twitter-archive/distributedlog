package com.twitter.distributedlog.benchmark;

import com.twitter.distributedlog.benchmark.utils.ShiftableRateLimiter;
import com.twitter.distributedlog.client.serverset.DLZkServerSet;
import com.twitter.distributedlog.service.TwitterServerSetUtils;
import com.twitter.finagle.stats.StatsReceiver;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.List;

/**
 * Twitter Writer Worker to handle serversets
 */
class TwitterWriterWorker extends WriterWorker {

    TwitterWriterWorker(String streamPrefix,
                        int startStreamId,
                        int endStreamId,
                        ShiftableRateLimiter rateLimiter,
                        int writeConcurrency,
                        int messageSizeBytes,
                        int batchSize,
                        int hostConnectionCoreSize,
                        int hostConnectionLimit,
                        List<String> serverSetPaths,
                        List<String> finagleNames,
                        StatsReceiver statsReceiver,
                        StatsLogger statsLogger,
                        boolean thriftmux,
                        boolean handshakeWithClientInfo,
                        int sendBufferSize,
                        int recvBufferSize,
                        boolean enableBatching) {
        super(streamPrefix,
                startStreamId,
                endStreamId,
                rateLimiter,
                writeConcurrency,
                messageSizeBytes,
                batchSize,
                hostConnectionCoreSize,
                hostConnectionLimit,
                serverSetPaths,
                finagleNames,
                statsReceiver,
                statsLogger,
                thriftmux,
                handshakeWithClientInfo,
                sendBufferSize,
                recvBufferSize,
                enableBatching);
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
