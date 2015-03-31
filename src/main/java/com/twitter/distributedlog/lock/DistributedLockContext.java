package com.twitter.distributedlog.lock;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashSet;
import java.util.Set;

class DistributedLockContext {
    private final Set<Pair<String, Long>> lockIds;

    DistributedLockContext() {
        this.lockIds = new HashSet<Pair<String, Long>>();
    }

    synchronized void addLockId(Pair<String, Long> lockId) {
        this.lockIds.add(lockId);
    }

    synchronized void clearLockIds() {
        this.lockIds.clear();
    }

    synchronized boolean hasLockId(Pair<String, Long> lockId) {
        return this.lockIds.contains(lockId);
    }
}
