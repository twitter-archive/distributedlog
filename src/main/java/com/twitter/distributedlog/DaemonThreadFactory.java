package com.twitter.distributedlog;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

class DaemonThreadFactory implements ThreadFactory {
    private ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
    private int priority = Thread.NORM_PRIORITY;
    public DaemonThreadFactory() {
    }
    public DaemonThreadFactory(int priority) {
        assert priority >= Thread.MIN_PRIORITY && priority <= Thread.MAX_PRIORITY;
        this.priority = priority;
    }
    public Thread newThread(Runnable r) {
        Thread thread = defaultThreadFactory.newThread(r);
        thread.setDaemon(true);
        thread.setPriority(priority);
        return thread;
    }
}
