package com.twitter.distributedlog.client.stats;

import com.twitter.finagle.stats.Counter;
import com.twitter.finagle.stats.StatsReceiver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stats Logger for ownerships
 */
public class OwnershipStatsLogger {

    public static class OwnershipStat {
        private final Counter hits;
        private final Counter misses;
        private final Counter removes;
        private final Counter redirects;
        private final Counter adds;

        OwnershipStat(StatsReceiver ownershipStats) {
            hits = ownershipStats.counter0("hits");
            misses = ownershipStats.counter0("misses");
            adds = ownershipStats.counter0("adds");
            removes = ownershipStats.counter0("removes");
            redirects = ownershipStats.counter0("redirects");
        }

        public void onHit() {
            hits.incr();
        }

        public void onMiss() {
            misses.incr();
        }

        public void onAdd() {
            adds.incr();
        }

        public void onRemove() {
            removes.incr();
        }

        public void onRedirect() {
            redirects.incr();
        }

    }

    private final OwnershipStat ownershipStat;
    private final StatsReceiver ownershipStatsReceiver;
    private final ConcurrentMap<String, OwnershipStat> ownershipStats =
            new ConcurrentHashMap<String, OwnershipStat>();

    public OwnershipStatsLogger(StatsReceiver statsReceiver,
                                StatsReceiver streamStatsReceiver) {
        this.ownershipStat = new OwnershipStat(statsReceiver.scope("ownership"));
        this.ownershipStatsReceiver = streamStatsReceiver.scope("perstream_ownership");
    }

    private OwnershipStat getOwnershipStat(String stream) {
        OwnershipStat stat = ownershipStats.get(stream);
        if (null == stat) {
            OwnershipStat newStat = new OwnershipStat(ownershipStatsReceiver.scope(stream));
            OwnershipStat oldStat = ownershipStats.putIfAbsent(stream, newStat);
            stat = null != oldStat ? oldStat : newStat;
        }
        return stat;
    }

    public void onMiss(String stream) {
        ownershipStat.onMiss();
        getOwnershipStat(stream).onMiss();
    }

    public void onHit(String stream) {
        ownershipStat.onHit();
        getOwnershipStat(stream).onHit();
    }

    public void onRedirect(String stream) {
        ownershipStat.onRedirect();
        getOwnershipStat(stream).onRedirect();
    }

    public void onRemove(String stream) {
        ownershipStat.onRemove();
        getOwnershipStat(stream).onRemove();
    }

    public void onAdd(String stream) {
        ownershipStat.onAdd();
        getOwnershipStat(stream).onAdd();
    }
}
