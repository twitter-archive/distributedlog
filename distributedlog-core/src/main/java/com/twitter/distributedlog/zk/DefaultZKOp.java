package com.twitter.distributedlog.zk;

import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;

/**
 * Default zookeeper operation. No action on commiting or aborting.
 */
public class DefaultZKOp extends ZKOp {

    public static DefaultZKOp of(Op op) {
        return new DefaultZKOp(op);
    }

    private DefaultZKOp(Op op) {
        super(op);
    }

    @Override
    protected void commitOpResult(OpResult opResult) {
        // no-op
    }

    @Override
    protected void abortOpResult(Throwable t, OpResult opResult) {
        // no-op
    }
}
