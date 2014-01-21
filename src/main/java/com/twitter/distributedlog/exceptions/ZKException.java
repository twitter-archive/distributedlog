package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class ZKException extends DLException {

    public ZKException(String msg) {
        super(StatusCode.ZOOKEEPER_ERROR, msg);
    }

    public ZKException(String msg, Throwable t) {
        super(StatusCode.ZOOKEEPER_ERROR, msg, t);
    }
}
