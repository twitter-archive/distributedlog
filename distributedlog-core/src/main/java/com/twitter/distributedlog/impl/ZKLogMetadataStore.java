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
package com.twitter.distributedlog.impl;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.metadata.LogMetadataStore;
import com.twitter.distributedlog.util.OrderedScheduler;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static com.twitter.distributedlog.impl.BKDLUtils.*;

/**
 * ZooKeeper based log metadata store
 */
public class ZKLogMetadataStore implements LogMetadataStore {

    final URI namespace;
    final Optional<URI> nsOptional;
    final ZooKeeperClient zkc;
    final ZKNamespaceWatcher nsWatcher;

    public ZKLogMetadataStore(
            DistributedLogConfiguration conf,
            URI namespace,
            ZooKeeperClient zkc,
            OrderedScheduler scheduler) {
        this.namespace = namespace;
        this.nsOptional = Optional.of(this.namespace);
        this.zkc = zkc;
        this.nsWatcher = new ZKNamespaceWatcher(conf, namespace, zkc, scheduler);
    }

    @Override
    public Future<URI> createLog(String logName) {
        return Future.value(namespace);
    }

    @Override
    public Future<Optional<URI>> getLogLocation(String logName) {
        return Future.value(nsOptional);
    }

    @Override
    public Future<Iterator<String>> getLogs() {
        final Promise<Iterator<String>> promise = new Promise<Iterator<String>>();
        final String nsRootPath = namespace.getPath();
        try {
            final ZooKeeper zk = zkc.get();
            zk.sync(nsRootPath, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int syncRc, String syncPath, Object ctx) {
                    if (KeeperException.Code.OK.intValue() == syncRc) {
                        zk.getChildren(nsRootPath, false, new AsyncCallback.Children2Callback() {
                            @Override
                            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                                if (KeeperException.Code.OK.intValue() == rc) {
                                    List<String> results = Lists.newArrayListWithExpectedSize(children.size());
                                    for (String child : children) {
                                        if (!isReservedStreamName(child)) {
                                            results.add(child);
                                        }
                                    }
                                    promise.setValue(results.iterator());
                                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                    List<String> streams = Lists.newLinkedList();
                                    promise.setValue(streams.iterator());
                                } else {
                                    promise.setException(new ZKException("Error reading namespace " + nsRootPath,
                                            KeeperException.Code.get(rc)));
                                }
                            }
                        }, null);
                    } else if (KeeperException.Code.NONODE.intValue() == syncRc) {
                        List<String> streams = Lists.newLinkedList();
                        promise.setValue(streams.iterator());
                    } else {
                        promise.setException(new ZKException("Error reading namespace " + nsRootPath,
                                KeeperException.Code.get(syncRc)));
                    }
                }
            }, null);
            zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        } catch (InterruptedException e) {
            promise.setException(e);
        }
        return promise;
    }

    @Override
    public void registerNamespaceListener(NamespaceListener listener) {
        this.nsWatcher.registerListener(listener);
    }
}
