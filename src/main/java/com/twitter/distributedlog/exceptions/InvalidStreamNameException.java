package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class InvalidStreamNameException extends DLException {

    private static final long serialVersionUID = 6393315766140568100L;

    public InvalidStreamNameException(String streamName) {
        super(StatusCode.INVALID_STREAM_NAME, "Invalid stream name : '" + streamName + "'");
    }

    public InvalidStreamNameException(String streamName, String reason) {
        super(StatusCode.INVALID_STREAM_NAME, "Invalid stream name : '" + streamName + "' : " + reason);
    }
}
