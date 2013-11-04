package com.twitter.distributedlog;

public class DistributedLogConstants {
    public static final long INVALID_TXID = -999;
    public static final long EMPTY_LEDGER_TX_ID = -99;
    public static final long MAX_TXID = Long.MAX_VALUE;
    public static final long SMALL_LEDGER_THRESHOLD = 10;
    public static final int LEDGER_METADATA_CURRENT_LAYOUT_VERSION = 2;
    public static final String DEFAULT_STREAM = "<default>";
    public static final int FIRST_LEDGER_METADATA_VERSION_FOR_LEDGER_SEQNO = 2;
    public static final long UNASSIGNED_LEDGER_SEQNO = 0;
    public static final long FIRST_LEDGER_SEQNO = 1;
    public static final long LOCK_IMMEDIATE = 0;
}
