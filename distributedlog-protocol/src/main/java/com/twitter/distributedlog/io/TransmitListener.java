package com.twitter.distributedlog.io;

import com.twitter.distributedlog.DLSN;

/**
 * Listener on transmit results.
 */
public interface TransmitListener {

    /**
     * Finalize the transmit result and result the last
     * {@link DLSN} in this transmit.
     *
     * @param lssn
     *          log segment sequence number
     * @param entryId
     *          entry id
     * @return last dlsn in this transmit
     */
    DLSN finalizeTransmit(long lssn, long entryId);

    /**
     * Complete the whole transmit.
     *
     * @param lssn
     *          log segment sequence number
     * @param entryId
     *          entry id
     */
    void completeTransmit(long lssn, long entryId);

    /**
     * Abort the transmit.
     *
     * @param reason
     *          reason to abort transmit
     */
    void abortTransmit(Throwable reason);
}
