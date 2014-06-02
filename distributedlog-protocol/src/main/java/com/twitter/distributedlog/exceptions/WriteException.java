package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class WriteException extends DLException {

    private static final long serialVersionUID = -1836146493446072122L;

    public WriteException(String stream, String transmitError) {
        super(StatusCode.WRITE_EXCEPTION,
            "Stream " + stream + " has already encountered an error : " +
                transmitError + " write rejected");
    }
}
