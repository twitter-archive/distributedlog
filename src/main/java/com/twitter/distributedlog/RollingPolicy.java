package com.twitter.distributedlog;

public interface RollingPolicy {
    /**
     * Determines if a rollover may be appropriate at this time.
     *
     * @param writer
     *          per stream log writer.
     * @param lastLedgerRollingTimeMillis
     *          last ledger rolling time in millis.
     * @return true if a rollover is required. otherwise, false.
     */
    boolean shouldRollLog(BKPerStreamLogWriter writer, long lastLedgerRollingTimeMillis);
}
