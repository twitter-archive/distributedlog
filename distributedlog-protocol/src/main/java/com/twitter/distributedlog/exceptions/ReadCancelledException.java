package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class ReadCancelledException extends DLException {

    private static final long serialVersionUID = -6273430297547510262L;

    public ReadCancelledException(String stream, String reason) {
        super(StatusCode.READ_CANCELLED_EXCEPTION,
              "Read cancelled on stream " + stream + " : " + reason);
    }
}
