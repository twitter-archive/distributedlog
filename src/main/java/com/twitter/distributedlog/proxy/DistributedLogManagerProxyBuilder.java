package com.twitter.distributedlog.proxy;

import com.google.common.base.Preconditions;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.Future;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DistributedLogManagerProxyBuilder {

    private String _name = null;
    private ClientId _clientId = null;
    private ServerSet _serverSet = null;
    private StatsReceiver _statsReceiver = new NullStatsReceiver();

    public static DistributedLogManagerProxyBuilder newBuilder() {
        return new DistributedLogManagerProxyBuilder();
    }

    private DistributedLogManagerProxyBuilder() {}

    public DistributedLogManagerProxyBuilder name(String name) {
        this._name = name;
        return this;
    }

    public DistributedLogManagerProxyBuilder clientId(ClientId clientId) {
        this._clientId = clientId;
        return this;
    }

    public DistributedLogManagerProxyBuilder serverSet(ServerSet serverSet) {
        this._serverSet = serverSet;
        return this;
    }

    public DistributedLogManagerProxyBuilder statsReceiver(StatsReceiver statsReceiver) {
        this._statsReceiver = statsReceiver;
        return this;
    }

    static class AsyncLogWriterImpl implements AsyncLogWriter {

        final String name;
        final DistributedLogClient client;

        AsyncLogWriterImpl(String name, DistributedLogClient client) {
            this.name = name;
            this.client = client;
        }

        @Override
        public Future<Boolean> truncate(DLSN dlsn) {
            return client.truncate(name, dlsn);
        }

        @Override
        public Future<DLSN> write(LogRecord record) {
            return client.write(name, ByteBuffer.wrap(record.getPayload()));
        }

        @Override
        public void close() {
            // nop
        }
    }

    public DistributedLogManagerProxy build() {
        Preconditions.checkNotNull(_name, "No name provided.");
        Preconditions.checkNotNull(_clientId, "No client id provided.");
        Preconditions.checkNotNull(_serverSet, "No cluster provided.");
        Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided.");

        final DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                .name(_name).clientId(_clientId).serverSet(_serverSet).statsReceiver(_statsReceiver).build();
        return new DistributedLogManagerProxy() {
            @Override
            public AsyncLogWriter getAsyncLogWriter(String streamName) {
                return new AsyncLogWriterImpl(streamName, client);
            }
        };
    }

}
