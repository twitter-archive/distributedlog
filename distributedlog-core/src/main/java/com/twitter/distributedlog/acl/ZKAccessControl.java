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
package com.twitter.distributedlog.acl;

import com.google.common.base.Objects;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.thrift.AccessControlEntry;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static com.google.common.base.Charsets.UTF_8;

public class ZKAccessControl {

    private static final int BUFFER_SIZE = 4096;

    public static final AccessControlEntry DEFAULT_ACCESS_CONTROL_ENTRY = new AccessControlEntry();

    public static class CorruptedAccessControlException extends IOException {

        private static final long serialVersionUID = 5391285182476211603L;

        public CorruptedAccessControlException(String zkPath, Throwable t) {
            super("Access Control @ " + zkPath + " is corrupted.", t);
        }
    }

    protected final AccessControlEntry accessControlEntry;
    protected final String zkPath;
    private int zkVersion;

    public ZKAccessControl(AccessControlEntry ace, String zkPath) {
        this(ace, zkPath, -1);
    }

    private ZKAccessControl(AccessControlEntry ace, String zkPath, int zkVersion) {
        this.accessControlEntry = ace;
        this.zkPath = zkPath;
        this.zkVersion = zkVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(zkPath, accessControlEntry);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ZKAccessControl)) {
            return false;
        }
        ZKAccessControl other = (ZKAccessControl) obj;
        return Objects.equal(zkPath, other.zkPath) &&
                Objects.equal(accessControlEntry, other.accessControlEntry);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("entry(path=").append(zkPath).append(", acl=")
                .append(accessControlEntry).append(")");
        return sb.toString();
    }

    public String getZKPath() {
        return zkPath;
    }

    public AccessControlEntry getAccessControlEntry() {
        return accessControlEntry;
    }

    public Future<ZKAccessControl> create(ZooKeeperClient zkc) {
        final Promise<ZKAccessControl> promise = new Promise<ZKAccessControl>();
        try {
            zkc.get().create(zkPath, serialize(accessControlEntry), zkc.getDefaultACL(), CreateMode.PERSISTENT,
                    new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            if (KeeperException.Code.OK.intValue() == rc) {
                                ZKAccessControl.this.zkVersion = 0;
                                promise.setValue(ZKAccessControl.this);
                            } else {
                                promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                            }
                        }
                    }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        } catch (InterruptedException e) {
            promise.setException(e);
        } catch (IOException e) {
            promise.setException(e);
        }
        return promise;
    }

    public Future<ZKAccessControl> update(ZooKeeperClient zkc) {
        final Promise<ZKAccessControl> promise = new Promise<ZKAccessControl>();
        try {
            zkc.get().setData(zkPath, serialize(accessControlEntry), zkVersion, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    if (KeeperException.Code.OK.intValue() == rc) {
                        ZKAccessControl.this.zkVersion = stat.getVersion();
                        promise.setValue(ZKAccessControl.this);
                    } else {
                        promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        } catch (InterruptedException e) {
            promise.setException(e);
        } catch (IOException e) {
            promise.setException(e);
        }
        return promise;
    }

    public static Future<ZKAccessControl> read(final ZooKeeperClient zkc, final String zkPath, Watcher watcher) {
        final Promise<ZKAccessControl> promise = new Promise<ZKAccessControl>();

        try {
            zkc.get().getData(zkPath, watcher, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    if (KeeperException.Code.OK.intValue() == rc) {
                        try {
                            AccessControlEntry ace = deserialize(zkPath, data);
                            promise.setValue(new ZKAccessControl(ace, zkPath, stat.getVersion()));
                        } catch (IOException ioe) {
                            promise.setException(ioe);
                        }
                    } else {
                        promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        } catch (InterruptedException e) {
            promise.setException(e);
        }
        return promise;
    }

    public static Future<Void> delete(final ZooKeeperClient zkc, final String zkPath) {
        final Promise<Void> promise = new Promise<Void>();

        try {
            zkc.get().delete(zkPath, -1, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (KeeperException.Code.OK.intValue() == rc ||
                            KeeperException.Code.NONODE.intValue() == rc) {
                        promise.setValue(null);
                    } else {
                        promise.setException(KeeperException.create(KeeperException.Code.get(rc)));
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.setException(e);
        } catch (InterruptedException e) {
            promise.setException(e);
        }
        return promise;
    }

    static byte[] serialize(AccessControlEntry ace) throws IOException {
        TMemoryBuffer transport = new TMemoryBuffer(BUFFER_SIZE);
        TJSONProtocol protocol = new TJSONProtocol(transport);
        try {
            ace.write(protocol);
            transport.flush();
            return transport.toString(UTF_8.name()).getBytes(UTF_8);
        } catch (TException e) {
            throw new IOException("Failed to serialize access control entry : ", e);
        } catch (UnsupportedEncodingException uee) {
            throw new IOException("Failed to serialize acesss control entry : ", uee);
        }
    }

    static AccessControlEntry deserialize(String zkPath, byte[] data) throws IOException {
        if (data.length == 0) {
            return DEFAULT_ACCESS_CONTROL_ENTRY;
        }

        AccessControlEntry ace = new AccessControlEntry();
        TMemoryInputTransport transport = new TMemoryInputTransport(data);
        TJSONProtocol protocol = new TJSONProtocol(transport);
        try {
            ace.read(protocol);
        } catch (TException e) {
            throw new CorruptedAccessControlException(zkPath, e);
        }
        return ace;
    }

}
