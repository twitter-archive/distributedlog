package com.twitter.distributedlog.service;

import com.twitter.finagle.thrift.ClientId$;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.util.Duration;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test Case of {@link com.twitter.distributedlog.service.DistributedLogClientBuilder}
 */
public class TestDistributedLogClientBuilder {

    @Test(timeout = 60000)
    public void testBuildClientsFromSameBuilder() throws Exception {
        DistributedLogClientBuilder builder = DistributedLogClientBuilder.newBuilder()
                .name("build-clients-from-same-builder")
                .clientId(ClientId$.MODULE$.apply("test-builder"))
                .finagleNameStr("inet!127.0.0.1:7001")
                .streamNameRegex(".*")
                .handshakeWithClientInfo(true)
                .clientBuilder(ClientBuilder.get()
                    .hostConnectionLimit(1)
                    .connectionTimeout(Duration.fromSeconds(1))
                    .requestTimeout(Duration.fromSeconds(10)));
        DistributedLogClient client1 = builder.build();
        DistributedLogClient client2 = builder.build();
        assertFalse(client1 == client2);
    }
}
