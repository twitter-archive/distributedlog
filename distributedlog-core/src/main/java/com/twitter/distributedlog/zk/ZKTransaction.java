package com.twitter.distributedlog.zk;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Transaction;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ZooKeeper Transaction
 */
public class ZKTransaction implements Transaction<Object>, AsyncCallback.MultiCallback {

    private final ZooKeeperClient zkc;
    private final List<ZKOp> ops;
    private final List<org.apache.zookeeper.Op> zkOps;
    private final Promise<Void> result;
    private final AtomicBoolean done = new AtomicBoolean(false);

    public ZKTransaction(ZooKeeperClient zkc) {
        this.zkc = zkc;
        this.ops = Lists.newArrayList();
        this.zkOps = Lists.newArrayList();
        this.result = new Promise<Void>();
    }

    @Override
    public void addOp(Op<Object> operation) {
        if (done.get()) {
            throw new IllegalStateException("Add an operation to a finished transaction");
        }
        assert(operation instanceof ZKOp);
        ZKOp zkOp = (ZKOp) operation;
        this.ops.add(zkOp);
        this.zkOps.add(zkOp.getOp());
    }

    @Override
    public Future<Void> execute() {
        if (!done.compareAndSet(false, true)) {
            return result;
        }
        try {
            zkc.get().multi(zkOps, this, result);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            result.setException(FutureUtils.zkException(e, ""));
        } catch (InterruptedException e) {
            result.setException(FutureUtils.zkException(e, ""));
        }
        return result;
    }

    @Override
    public void abort(Throwable cause) {
        if (!done.compareAndSet(false, true)) {
            return;
        }
        for (int i = 0; i < ops.size(); i++) {
            ops.get(i).abortOpResult(cause, null);
        }
        FutureUtils.setException(result, cause);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<OpResult> results) {
        if (KeeperException.Code.OK.intValue() == rc) { // transaction succeed
            for (int i = 0; i < ops.size(); i++) {
                ops.get(i).commitOpResult(results.get(i));
            }
            FutureUtils.setValue(result, null);
        } else {
            KeeperException ke = KeeperException.create(KeeperException.Code.get(rc));
            for (int i = 0; i < ops.size(); i++) {
                ops.get(i).abortOpResult(ke, null != results ? results.get(i) : null);
            }
            FutureUtils.setException(result, ke);
        }
    }
}
