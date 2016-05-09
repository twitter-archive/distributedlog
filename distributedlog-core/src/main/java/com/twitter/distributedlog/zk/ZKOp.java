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

import com.twitter.distributedlog.util.Transaction;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;

import javax.annotation.Nullable;

/**
 * ZooKeeper Transaction Operation
 */
public abstract class ZKOp implements Transaction.Op<Object> {

    protected final Op op;

    protected ZKOp(Op op) {
        this.op = op;
    }

    public Op getOp() {
        return op;
    }

    @Override
    public void commit(Object r) {
        assert(r instanceof OpResult);
        commitOpResult((OpResult) r);
    }

    protected abstract void commitOpResult(OpResult opResult);

    @Override
    public void abort(Throwable t, Object r) {
        assert(r instanceof OpResult);
        abortOpResult(t, (OpResult) r);
    }

    /**
     * Abort the operation with exception <i>t</i> and result <i>opResult</i>.
     *
     * @param t the reason to abort the operation
     * @param opResult the result of operation
     */
    protected abstract void abortOpResult(Throwable t,
                                          @Nullable OpResult opResult);
}
