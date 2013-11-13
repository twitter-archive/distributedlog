package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;

import java.io.IOException;

public abstract class DLException extends IOException {
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
        switch (response.getCode()) {
            case FOUND:
                String errMsg;
                if (response.isSetErrMsg()) {
                    errMsg = response.getErrMsg();
                } else {
                    errMsg = "Request is redirected to " + response.getLocation();
                }
                return new OwnershipAcquireFailedException(errMsg, response.getLocation());
            case SUCCESS:
                throw new IllegalArgumentException("Can't instantiate an exception for success response.");
            // TODO: we need more specific exceptions
            default:
                if (response.isSetErrMsg()) {
                    return new InternalServerException(response.getErrMsg());
                } else {
                    return new InternalServerException("Internal Server Error");
                }
        }
    }
}
