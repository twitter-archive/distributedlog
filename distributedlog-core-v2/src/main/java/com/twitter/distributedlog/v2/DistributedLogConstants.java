package com.twitter.distributedlog.v2;

import com.google.common.collect.ImmutableList;
import org.apache.bookkeeper.shims.Version;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.List;

public class DistributedLogConstants {
    public static final long INVALID_TXID = -999;
    public static final long EMPTY_LEDGER_TX_ID = -99;
    public static final long MAX_TXID = Long.MAX_VALUE;
    public static final long SMALL_LEDGER_THRESHOLD = 10;
    public static final int LAYOUT_VERSION = 1;
    // Allow 4K overhead for metadata within the max transmission size
    public static final int MAX_LOGRECORD_SIZE = 1 * 1024 * 1024 - 8 * 1024; //1MB - 8KB
    // Allow 4K overhead for transmission overhead
    public static final int MAX_TRANSMISSION_SIZE = 1 * 1024 * 1024 - 4 * 1024; //1MB - 4KB
    public static final long LOCK_IMMEDIATE = 0;
    public static final long LOCK_TIMEOUT_INFINITE = -1;
    public static final String UNKNOWN_CLIENT_ID = "Unknown-ClientId";
    public static final int INPUTSTREAM_MARK_LIMIT = 16;
    
    // An ACL that gives all permissions to node creators and read permissions only to everyone else.
    public static final List<ACL> EVERYONE_READ_CREATOR_ALL =
        ImmutableList.<ACL>builder()
            .addAll(Ids.CREATOR_ALL_ACL)
            .addAll(Ids.READ_ACL_UNSAFE)
            .build();
}
