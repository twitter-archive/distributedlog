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
package com.twitter.distributedlog.zk;

import com.twitter.distributedlog.util.Transaction.OpListener;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;

import javax.annotation.Nullable;

/**
 * ZooKeeper Operation that plays with {@link org.apache.bookkeeper.versioning.Version}
 */
public class ZKVersionedSetOp extends ZKOp {

    private final OpListener<Version> listener;

    public ZKVersionedSetOp(Op op, OpListener<Version> opListener) {
        super(op);
        this.listener = opListener;
    }

    @Override
    protected void commitOpResult(OpResult opResult) {
        assert(opResult instanceof OpResult.SetDataResult);
        OpResult.SetDataResult setDataResult = (OpResult.SetDataResult) opResult;
        listener.onCommit(new ZkVersion(setDataResult.getStat().getVersion()));
    }

    @Override
    protected void abortOpResult(Throwable t,
                                 @Nullable OpResult opResult) {
        Throwable cause;
        if (null == opResult) {
            cause = t;
        } else {
            assert (opResult instanceof OpResult.ErrorResult);
            OpResult.ErrorResult errorResult = (OpResult.ErrorResult) opResult;
            if (KeeperException.Code.OK.intValue() == errorResult.getErr()) {
                cause = t;
            } else {
                cause = KeeperException.create(KeeperException.Code.get(errorResult.getErr()));
            }
        }
        listener.onAbort(cause);
    }

}
