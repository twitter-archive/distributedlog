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
package com.twitter.distributedlog.subscription;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.util.Function;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ZooKeeper Based Subscriptions Store.
 */
public class ZKSubscriptionsStore implements SubscriptionsStore {

    private final ZooKeeperClient zkc;
    private final String zkPath;
    private final ConcurrentMap<String, ZKSubscriptionStateStore> subscribers =
            new ConcurrentHashMap<String, ZKSubscriptionStateStore>();

    public ZKSubscriptionsStore(ZooKeeperClient zkc, String zkPath) {
        this.zkc = zkc;
        this.zkPath = zkPath;
    }

    private ZKSubscriptionStateStore getSubscriber(String subscriberId) {
        ZKSubscriptionStateStore ss = subscribers.get(subscriberId);
        if (ss == null) {
            ZKSubscriptionStateStore newSS = new ZKSubscriptionStateStore(zkc,
                    String.format("%s/%s", zkPath, subscriberId));
            ZKSubscriptionStateStore oldSS = subscribers.putIfAbsent(subscriberId, newSS);
            if (oldSS == null) {
                ss = newSS;
            } else {
                try {
                    newSS.close();
                } catch (IOException e) {
                    // ignore the exception
                }
                ss = oldSS;
            }
        }
        return ss;
    }

    @Override
    public Future<DLSN> getLastCommitPosition(String subscriberId) {
        return getSubscriber(subscriberId).getLastCommitPosition();
    }

    @Override
    public Future<Map<String, DLSN>> getLastCommitPositions() {
        final Promise<Map<String, DLSN>> result = new Promise<Map<String, DLSN>>();
        try {
            this.zkc.get().getChildren(this.zkPath, false, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                    if (KeeperException.Code.NONODE.intValue() == rc) {
                        result.setValue(new HashMap<String, DLSN>());
                    } else if (KeeperException.Code.OK.intValue() != rc) {
                        result.setException(KeeperException.create(KeeperException.Code.get(rc), path));
                    } else {
                        getLastCommitPositions(result, children);
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            result.setException(zkce);
        } catch (InterruptedException ie) {
            result.setException(new DLInterruptedException("getLastCommitPositions was interrupted", ie));
        }
        return result;
    }

    private void getLastCommitPositions(final Promise<Map<String, DLSN>> result,
                                        List<String> subscribers) {
        List<Future<Pair<String, DLSN>>> futures =
                new ArrayList<Future<Pair<String, DLSN>>>(subscribers.size());
        for (String s : subscribers) {
            final String subscriber = s;
            Future<Pair<String, DLSN>> future =
                // Get the last commit position from zookeeper
                getSubscriber(subscriber).getLastCommitPositionFromZK().map(
                        new AbstractFunction1<DLSN, Pair<String, DLSN>>() {
                            @Override
                            public Pair<String, DLSN> apply(DLSN dlsn) {
                                return Pair.of(subscriber, dlsn);
                            }
                        });
            futures.add(future);
        }
        Future.collect(futures).foreach(
            new AbstractFunction1<List<Pair<String, DLSN>>, BoxedUnit>() {
                @Override
                public BoxedUnit apply(List<Pair<String, DLSN>> subscriptions) {
                    Map<String, DLSN> subscriptionMap = new HashMap<String, DLSN>();
                    for (Pair<String, DLSN> pair : subscriptions) {
                        subscriptionMap.put(pair.getLeft(), pair.getRight());
                    }
                    result.setValue(subscriptionMap);
                    return BoxedUnit.UNIT;
                }
            });
    }

    @Override
    public Future<BoxedUnit> advanceCommitPosition(String subscriberId, DLSN newPosition) {
        return getSubscriber(subscriberId).advanceCommitPosition(newPosition);
    }

    @Override
    public void close() throws IOException {
        // no-op
        for (SubscriptionStateStore store : subscribers.values()) {
            store.close();
        }
    }

}
