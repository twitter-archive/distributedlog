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

import java.nio.ByteBuffer;

public class DistributedLogManagerProxyBuilder {

    private String _name = null;
    private ClientId _clientId = null;
    private ServerSet _serverSet = null;
    private StatsReceiver _statsReceiver = null;
    private DistributedLogClientBuilder _clientBuilder = null;

    public static DistributedLogManagerProxyBuilder newBuilder() {
        return new DistributedLogManagerProxyBuilder();
    }

    private DistributedLogManagerProxyBuilder() {}

    /**
     * Use {@link #clientBuilder(com.twitter.distributedlog.service.DistributedLogClientBuilder)}.
     *
     * @param name
     *          proxy name.
     * @return proxy builder.
     */
    @Deprecated
    public DistributedLogManagerProxyBuilder name(String name) {
        this._name = name;
        return this;
    }

    /**
     * Use {@link #clientBuilder(com.twitter.distributedlog.service.DistributedLogClientBuilder)}.
     *
     * @param clientId
     *          client id.
     * @return proxy builder.
     */
    @Deprecated
    public DistributedLogManagerProxyBuilder clientId(ClientId clientId) {
        this._clientId = clientId;
        return this;
    }

    /**
     * Use {@link #clientBuilder(com.twitter.distributedlog.service.DistributedLogClientBuilder)}.
     *
     * @param serverSet
     *          server set.
     * @return proxy builder.
     */
    @Deprecated
    public DistributedLogManagerProxyBuilder serverSet(ServerSet serverSet) {
        this._serverSet = serverSet;
        return this;
    }

    /**
     * Use {@link #clientBuilder(com.twitter.distributedlog.service.DistributedLogClientBuilder)}.
     *
     * @param statsReceiver
     *          stats receiver.
     * @return proxy builder
     */
    @Deprecated
    public DistributedLogManagerProxyBuilder statsReceiver(StatsReceiver statsReceiver) {
        this._statsReceiver = statsReceiver;
        return this;
    }

    /**
     * Build the proxy with given <i>clientBuilder</i>.
     *
     * @see com.twitter.distributedlog.service.DistributedLogClientBuilder
     * @param clientBuilder
     *          client builder.
     * @return proxy builder.
     */
    public DistributedLogManagerProxyBuilder clientBuilder(DistributedLogClientBuilder clientBuilder) {
        this._clientBuilder = clientBuilder;
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
        if (null == _clientBuilder) {
            Preconditions.checkNotNull(_name, "No name provided.");
            Preconditions.checkNotNull(_clientId, "No client id provided.");
            Preconditions.checkNotNull(_serverSet, "No cluster provided.");
            Preconditions.checkNotNull(_statsReceiver, "No stats receiver provided.");
        }

        final DistributedLogClient client;

        if (null == _clientBuilder) {
            client = DistributedLogClientBuilder.newBuilder()
                    .name(_name).clientId(_clientId).serverSet(_serverSet).statsReceiver(_statsReceiver).build();
        } else {
            if (null != _name) {
                _clientBuilder.name(_name);
            }
            if (null != _clientId) {
                _clientBuilder.clientId(_clientId);
            }
            if (null != _serverSet) {
                _clientBuilder.serverSet(_serverSet);
            }
            if (null != _statsReceiver) {
                _clientBuilder.statsReceiver(_statsReceiver);
            }
            client = _clientBuilder.build();
        }
        return new DistributedLogManagerProxy() {
            @Override
            public AsyncLogWriter getAsyncLogWriter(String streamName) {
                return new AsyncLogWriterImpl(streamName, client);
            }
        };
    }

}
