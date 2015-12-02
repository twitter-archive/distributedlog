package com.twitter.distributedlog.v2;

public interface ReaderNotification {
    void notifyNextRecordAvailable();
}
