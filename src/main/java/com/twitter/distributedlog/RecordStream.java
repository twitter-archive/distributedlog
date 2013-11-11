package com.twitter.distributedlog;

interface RecordStream {
    DLSN advanceToNextRecord();
}
