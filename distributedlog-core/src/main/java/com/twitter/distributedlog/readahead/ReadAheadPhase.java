package com.twitter.distributedlog.readahead;

/**
 * Enum code represents readahead phases
 */
public enum ReadAheadPhase {
    ERROR(-5),
    TRUNCATED(-4),
    INTERRUPTED(-3),
    STOPPED(-2),
    EXCEPTION_HANDLING(-1),
    SCHEDULE_READAHEAD(0),
    GET_LEDGERS(1),
    OPEN_LEDGER(2),
    CLOSE_LEDGER(3),
    READ_LAST_CONFIRMED(4),
    READ_ENTRIES(5);

    int code;

    ReadAheadPhase(int code) {
        this.code = code;
    }

    int getCode() {
        return this.code;
    }
}
