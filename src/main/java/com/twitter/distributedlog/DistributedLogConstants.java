package com.twitter.distributedlog;

public class DistributedLogConstants {
    public static final long INVALID_TXID = -999;
    public static final long EMPTY_LEDGER_TX_ID = -99;
    public static final long MAX_TXID = Long.MAX_VALUE;
    public static final long SMALL_LEDGER_THRESHOLD = 10;
    public static final int LAYOUT_VERSION = 1;
    public static final String DEFAULT_STREAM = "<default>";
    // Allow 4K overhead for metadata within the max transmission size
    public static final int MAX_LOGRECORD_SIZE = 1 * 1024 * 1024 - 8 * 1024; //1MB - 8KB
    // Allow 4K overhead for transmission overhead
    public static final int MAX_TRANSMISSION_SIZE = 1 * 1024 * 1024 - 4 * 1024; //1MB - 4KB
    public static final long LOCK_IMMEDIATE = 0;
    public static final long LOCK_TIMEOUT_INFINITE = -1;
    public static final String UNKNOWN_CLIENT_ID = "Unknown-ClientId";
    public static final int INPUTSTREAM_MARK_LIMIT = 16;
}
