package com.twitter.distributedlog.util;

public class SyncObject<T> {
    int i;
    int rc;
    T value = null;

    public synchronized void inc() {
        i++;
    }

    public synchronized void dec() {
        i--;
        notifyAll();
    }

    public synchronized void block(int limit) throws InterruptedException {
        while (i > limit) {
            int prev = i;
            wait();
            if (i == prev) {
                break;
            }
        }
    }

    public void setrc(int rc) {
        this.rc = rc;
    }

    public int getrc() {
        return rc;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public T getValue() {
        return this.value;
    }

}
