package com.twitter.distributedlog.zk;

import com.twitter.distributedlog.exceptions.ZKException;
import com.twitter.distributedlog.util.Transaction.OpListener;
import org.apache.bookkeeper.meta.ZkVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;

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
    protected void abortOpResult(OpResult opResult) {
        assert(opResult instanceof OpResult.ErrorResult);
        OpResult.ErrorResult errorResult = (OpResult.ErrorResult) opResult;
        listener.onAbort(KeeperException.create(KeeperException.Code.get(errorResult.getErr())));
    }

}
