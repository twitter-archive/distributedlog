package com.twitter.distributedlog;

class SyncObject<T> {
    int i;
    int rc;
    T value = null;

    synchronized void inc() {
        i++;
    }

    synchronized void dec() {
        i--;
        notifyAll();
    }

    synchronized void block(int limit) throws InterruptedException {
        while (i > limit) {
            int prev = i;
            wait();
            if (i == prev) {
                break;
            }
        }
    }

    void setrc(int rc) {
        this.rc = rc;
    }

    int getrc() {
        return rc;
    }

    void setValue(T value) {
        this.value = value;
    }

    T getValue() {
        return this.value;
    }

}
