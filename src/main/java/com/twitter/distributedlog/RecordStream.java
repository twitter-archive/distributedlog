package com.twitter.distributedlog;

interface RecordStream {
    void advanceToNextRecord();
    DLSN getCurrentPosition();
    String getName();
}
