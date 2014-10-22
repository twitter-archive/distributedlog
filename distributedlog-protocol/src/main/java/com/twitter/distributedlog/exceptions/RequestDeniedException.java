package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class RequestDeniedException extends DLException {

    private static final long serialVersionUID = 7338220414584728216L;

    public RequestDeniedException(String stream, String operation) {
        super(StatusCode.REQUEST_DENIED,
                operation + " request to stream " + stream + " is denied");
    }
}
