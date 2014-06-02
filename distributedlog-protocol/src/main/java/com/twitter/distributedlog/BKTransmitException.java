package com.twitter.distributedlog;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.thrift.service.StatusCode;

/**
 * Thrown when the send to bookkeeper fails
 * This is thrown by the next attempt to write, send or flush
 */
public class BKTransmitException extends DLException {

    private static final long serialVersionUID = -5796100450432076091L;

    final int bkRc;

    public BKTransmitException(String message, int bkRc) {
        super(StatusCode.BK_TRANSMIT_ERROR, message + " : " + bkRc);
        this.bkRc = bkRc;
    }

    public int getBKResultCode() {
        return this.bkRc;
    }

}
