package com.twitter.distributedlog.exceptions;

import org.apache.bookkeeper.client.BKException;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class WriteException extends DLException {

    private static final long serialVersionUID = -1836146493446072122L;

    public WriteException(String stream, int transmitResult) {
        super(StatusCode.WRITE_EXCEPTION,
            "Stream has already encountered an error : " +
                BKException.getMessage(transmitResult) + " write rejected");
    }
}
