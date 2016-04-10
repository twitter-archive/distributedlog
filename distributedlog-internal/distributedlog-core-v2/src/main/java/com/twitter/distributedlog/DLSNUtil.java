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

    public static int getRecordPersistentSize(LogRecord record) {
        return record.getPersistentSize();
    }

    public static int getPositionWithinLogSegment(LogRecord record) {
        return record.getPositionWithinLogSegment();
    }

    public static boolean isEndOfStream(LogRecord record) {
        return record.isEndOfStream();
    }

    public static void setPositionWithinLogSegment(LogRecord record,
                                                   int positionWithinLogSegment) {
        record.setPositionWithinLogSegment(positionWithinLogSegment);
    }

    public static void setControlRecord(LogRecord record) {
        record.setControl();;
    }

    public static void setEndOfStream(LogRecord record) {
        record.setEndOfStream();
    }

}
