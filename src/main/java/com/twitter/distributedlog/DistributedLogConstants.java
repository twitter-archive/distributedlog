package com.twitter.distributedlog;

import org.apache.bookkeeper.shims.Version;

import java.util.concurrent.TimeUnit;

import com.twitter.distributedlog.exceptions.InternalServerException;

public class DistributedLogConstants {
    public static final long INVALID_TXID = -999;
    public static final long EMPTY_LEDGER_TX_ID = -99;
    public static final long MAX_TXID = Long.MAX_VALUE;
    public static final long SMALL_LEDGER_THRESHOLD = 10;
    public static final int LEDGER_METADATA_CURRENT_LAYOUT_VERSION = 2;
    public static final int LOGSEGMENT_NAME_VERSION = 1;
    // Allow 4K overhead for metadata within the max transmission size
    public static final int MAX_LOGRECORD_SIZE = 1 * 1024 * 1024 - 8 * 1024; //1MB - 8KB
    // Allow 4K overhead for transmission overhead
    public static final int MAX_TRANSMISSION_SIZE = 1 * 1024 * 1024 - 4 * 1024; //1MB - 4KB
    public static final long LOCK_IMMEDIATE = 0;
    public static final long LOCK_TIMEOUT_INFINITE = -1;
    public static final String UNKNOWN_CLIENT_ID = "Unknown-ClientId";
    public static final int LOCAL_REGION_ID = 0;
    public static final int INPUTSTREAM_MARK_LIMIT = 16;
    public static final int FIRST_LEDGER_METADATA_VERSION_FOR_LEDGER_SEQNO = 2;
    public static final long UNASSIGNED_LEDGER_SEQNO = 0;
    public static final long FIRST_LEDGER_SEQNO = 1;
    public static final long UNRESOLVED_LEDGER_ID = -1;
    public static final long LATENCY_WARN_THRESHOLD_IN_MILLIS = TimeUnit.SECONDS.toMillis(1);
    public static final int DL_INTERRUPTED_EXCEPTION_RESULT_CODE = Integer.MIN_VALUE + 1;

    public static final String ALLOCATION_POOL_NODE = ".allocation_pool";
    // log segment prefix
    public static final String INPROGRESS_LOGSEGMENT_PREFIX = "inprogress";
    public static final String COMPLETED_LOGSEGMENT_PREFIX = "logrecs";
    // ZK Version
    public static final Version ZK_VERSION = new Version("zk");
    public static final String ZK33 = "3.3";
    public static final String ZK34 = "3.4";
}
