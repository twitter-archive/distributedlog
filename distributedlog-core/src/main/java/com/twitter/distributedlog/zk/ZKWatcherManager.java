package com.twitter.distributedlog.zk;

import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Watcher Manager to manage watchers
 */
public class ZKWatcherManager implements Watcher {

    static final Logger logger = LoggerFactory.getLogger(ZKWatcherManager.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String _name;
        private StatsLogger _statsLogger;

        public Builder name(String name) {
            this._name = name;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this._statsLogger = statsLogger;
            return this;
        }

        public ZKWatcherManager build() {
            return new ZKWatcherManager(_name, _statsLogger);
        }
    }

    private final String name;
    private final StatsLogger statsLogger;

    protected final ConcurrentMap<String, Set<Watcher>> childWatches;
    protected final AtomicInteger allWatchesGauge;

    private ZKWatcherManager(String name,
                             StatsLogger statsLogger) {
        this.name = name;
        this.statsLogger = statsLogger;

        // watches
        this.childWatches = new ConcurrentHashMap<String, Set<Watcher>>();
        this.allWatchesGauge = new AtomicInteger(0);

        // stats
        this.statsLogger.registerGauge("total_watches", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return allWatchesGauge.get();
            }
        });

        this.statsLogger.registerGauge("num_child_watches", new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return childWatches.size();
            }
        });
    }

    public Watcher registerChildWatcher(String path, Watcher watcher) {
        Set<Watcher> watchers = childWatches.get(path);
        if (null == watchers) {
            Set<Watcher> newWatchers = new HashSet<Watcher>();
            Set<Watcher> oldWatchers = childWatches.putIfAbsent(path, newWatchers);
            watchers = (null == oldWatchers) ? newWatchers : oldWatchers;
        }
        synchronized (watchers) {
            if (childWatches.get(path) == watchers) {
                if (watchers.add(watcher)) {
                    allWatchesGauge.incrementAndGet();
                }
            } else {
                logger.warn("Watcher set for path {} has been changed while registering child watcher {}.",
                        path, watcher);
            }
        }
        return this;
    }

    public void unregisterChildWatcher(String path, Watcher watcher) {
        Set<Watcher> watchers = childWatches.get(path);
        if (null == watchers) {
            logger.warn("No watchers found on path {} while unregistering child watcher {}.",
                    path, watcher);
            return;
        }
        synchronized (watchers) {
            if (watchers.remove(watcher)) {
                allWatchesGauge.decrementAndGet();
            } else {
                logger.warn("Remove a non-registered child watcher {} from path {}", watcher, path);
            }
            if (watchers.isEmpty()) {
                childWatches.remove(path, watchers);
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                handleKeeperStateEvent(event);
                break;
            case NodeChildrenChanged:
                handleChildWatchEvent(event);
                break;
            default:
                break;
        }
    }

    private void handleKeeperStateEvent(WatchedEvent event) {
        Set<Watcher> savedAllWatches = new HashSet<Watcher>(allWatchesGauge.get());
        for (Set<Watcher> watcherSet : childWatches.values()) {
            synchronized (watcherSet) {
                savedAllWatches.addAll(watcherSet);
            }
        }
        for (Watcher watcher : savedAllWatches) {
            watcher.process(event);
        }
    }

    private void handleChildWatchEvent(WatchedEvent event) {
        String path = event.getPath();
        if (null == path) {
            logger.warn("Received zookeeper watch event with null path : {}", event);
            return;
        }
        Set<Watcher> watchers = childWatches.get(path);
        if (null == watchers) {
            return;
        }
        Set<Watcher> watchersToFire;
        synchronized (watchers) {
            watchersToFire = new HashSet<Watcher>(watchers.size());
            watchersToFire.addAll(watchers);
        }
        for (Watcher watcher : watchersToFire) {
            watcher.process(event);
        }
    }
}
