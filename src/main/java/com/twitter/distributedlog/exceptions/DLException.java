package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;

import java.io.IOException;

public class DLException extends IOException {
    private static final long serialVersionUID = -4485775468586114393L;
    protected final StatusCode code;

    protected DLException(StatusCode code) {
        super();
        this.code = code;
    }

    protected DLException(StatusCode code, String msg) {
        super(msg);
        this.code = code;
    }

    protected DLException(StatusCode code, Throwable t) {
        super(t);
        this.code = code;
    }

    protected DLException(StatusCode code, String msg, Throwable t) {
        super(msg, t);
        this.code = code;
    }

    /**
     * Return the status code representing the exception.
     *
     * @return status code representing the exception.
     */
    public StatusCode getCode() {
        return code;
    }

    public static DLException of(ResponseHeader response) {
        String errMsg;
        switch (response.getCode()) {
            case FOUND:
                if (response.isSetErrMsg()) {
                    errMsg = response.getErrMsg();
                } else {
                    errMsg = "Request is redirected to " + response.getLocation();
                }
                return new OwnershipAcquireFailedException(errMsg, response.getLocation());
            case SUCCESS:
                throw new IllegalArgumentException("Can't instantiate an exception for success response.");
            default:
                if (response.isSetErrMsg()) {
                    errMsg = response.getErrMsg();
                } else {
                    errMsg = response.getCode().name();
                }
                return new DLException(response.getCode(), errMsg);
        }
    }
}
