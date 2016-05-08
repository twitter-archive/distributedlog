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
