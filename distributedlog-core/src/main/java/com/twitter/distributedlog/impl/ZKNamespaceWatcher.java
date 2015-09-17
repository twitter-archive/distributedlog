package com.twitter.distributedlog.impl;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.namespace.NamespaceWatcher;
import com.twitter.distributedlog.util.OrderedScheduler;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.twitter.distributedlog.impl.BKDLUtils.*;

/**
 * Watcher on watching a given namespace
 */
public class ZKNamespaceWatcher extends NamespaceWatcher
        implements Runnable, Watcher, AsyncCallback.Children2Callback {

    static final Logger logger = LoggerFactory.getLogger(ZKNamespaceWatcher.class);

    private final DistributedLogConfiguration conf;
    private final URI uri;
    private final ZooKeeperClient zkc;
    private final OrderedScheduler scheduler;
    private final AtomicBoolean namespaceWatcherSet = new AtomicBoolean(false);

    public ZKNamespaceWatcher(DistributedLogConfiguration conf,
                              URI uri,
                              ZooKeeperClient zkc,
                              OrderedScheduler scheduler) {
        this.conf = conf;
        this.uri = uri;
        this.zkc = zkc;
        this.scheduler = scheduler;
    }

    private void scheduleTask(Runnable r, long ms) {
        try {
            scheduler.schedule(r, ms, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            logger.error("Task {} scheduled in {} ms is rejected : ", new Object[]{r, ms, ree});
        }
    }

    @Override
    public void run() {
        try {
            doWatchNamespaceChanges();
        } catch (Exception e) {
            logger.error("Encountered unknown exception on watching namespace {} ", uri, e);
        }
    }

    public void watchNamespaceChanges() {
        if (!namespaceWatcherSet.compareAndSet(false, true)) {
            return;
        }
        doWatchNamespaceChanges();
    }

    private void doWatchNamespaceChanges() {
        try {
            zkc.get().getChildren(uri.getPath(), this, this, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        } catch (InterruptedException e) {
            logger.warn("Interrupted on watching namespace changes for {} : ", uri, e);
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        if (KeeperException.Code.OK.intValue() == rc) {
            logger.info("Received updated logs under {} : {}", uri, children);
            List<String> result = new ArrayList<String>(children.size());
            for (String s : children) {
                if (isReservedStreamName(s)) {
                    continue;
                }
                result.add(s);
            }
            for (NamespaceListener listener : listeners) {
                listener.onStreamsChanged(result.iterator());
            }
        } else {
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            if (event.getState() == Event.KeeperState.Expired) {
                scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
            }
            return;
        }
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            // watch namespace changes again.
            doWatchNamespaceChanges();
        }
    }
}
