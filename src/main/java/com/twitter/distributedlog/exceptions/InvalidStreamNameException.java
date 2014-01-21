package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class InvalidStreamNameException extends DLException {

    public InvalidStreamNameException(String streamName) {
        super(StatusCode.INVALID_STREAM_NAME, "Invalid stream name : '" + streamName + "'");
    }

    public InvalidStreamNameException(String streamName, String reason) {
        super(StatusCode.INVALID_STREAM_NAME, "Invalid stream name : '" + streamName + "' : " + reason);
    }
}
