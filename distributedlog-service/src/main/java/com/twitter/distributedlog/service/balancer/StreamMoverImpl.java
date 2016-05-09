/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.service.balancer;

import com.twitter.distributedlog.client.monitor.MonitorServiceClient;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Move Streams from <i>src</i> to <i>target</i>.
 */
public class StreamMoverImpl implements StreamMover {

    private static final Logger logger = LoggerFactory.getLogger(StreamMoverImpl.class);

    final String source, target;
    final DistributedLogClient srcClient, targetClient;
    final MonitorServiceClient srcMonitor, targetMonitor;

    public StreamMoverImpl(String source, DistributedLogClient srcClient, MonitorServiceClient srcMonitor,
                           String target, DistributedLogClient targetClient, MonitorServiceClient targetMonitor) {
        this.source = source;
        this.srcClient = srcClient;
        this.srcMonitor = srcMonitor;
        this.target = target;
        this.targetClient = targetClient;
        this.targetMonitor = targetMonitor;
    }

    /**
     * Move given stream <i>streamName</i>
     *
     * @param streamName
     *          stream name to move
     * @return <i>true</i> if successfully moved the stream, <i>false</i> when failure happens.
     * @throws Exception
     */
    public boolean moveStream(final String streamName) {
        try {
            doMoveStream(streamName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void doMoveStream(final String streamName) throws Exception {
        Await.result(srcClient.release(streamName).flatMap(new Function<Void, Future<Void>>() {
            @Override
            public Future<Void> apply(Void result) {
                return targetMonitor.check(streamName).addEventListener(new FutureEventListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        logger.info("Moved stream {} from {} to {}.",
                                new Object[]{streamName, source, target});
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        logger.info("Failed to move stream {} from region {} to {} : ",
                                new Object[]{streamName, source, target, cause});
                    }
                });
            }
        }));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StreamMover('").append(source).append("' -> '").append(target).append("')");
        return sb.toString();
    }
}
