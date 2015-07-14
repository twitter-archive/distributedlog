package com.twitter.distributedlog.client.proxy;

import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;

/**
 * Mock Thrift Client
 */
class MockThriftClient extends Service<ThriftClientRequest,byte[]> {
    @Override
    public Future<byte[]> apply(ThriftClientRequest request) {
        return Future.value(request.message);
    }
}
