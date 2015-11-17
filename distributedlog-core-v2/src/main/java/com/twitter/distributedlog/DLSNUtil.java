package com.twitter.distributedlog;

/**
 * Utils to access internal dlsn for v2 readers
 */
public class DLSNUtil {

    public static long getEntryId(DLSN dlsn) {
        return dlsn.getEntryId();
    }

    public static long getSlotId(DLSN dlsn) {
        return dlsn.getSlotId();
    }
}
