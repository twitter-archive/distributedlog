package com.twitter.distributedlog.zk;

import org.apache.zookeeper.data.Stat;

/**
 * Holder class for zookeeper data & its stat
 */
public class DataWithStat {
    byte[] data = null;
    Stat stat = null;

    synchronized public void setDataWithStat(byte[] data, Stat stat) {
        this.data = data;
        this.stat = stat;
    }

    synchronized public boolean exists() {
        return null != this.stat;
    }

    synchronized public boolean notExists() {
        return null == this.stat;
    }

    synchronized public byte[] getData() {
        return this.data;
    }

    synchronized public Stat getStat() {
        return this.stat;
    }
}
