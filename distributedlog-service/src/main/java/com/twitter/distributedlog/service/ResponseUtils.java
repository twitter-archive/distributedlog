package com.twitter.distributedlog.service;

import com.twitter.distributedlog.exceptions.DLException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.thrift.service.BulkWriteResponse;
import com.twitter.distributedlog.thrift.service.ResponseHeader;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.distributedlog.thrift.service.WriteResponse;

/**
 * Utility methods for building write proxy service responses.
 */
public class ResponseUtils {
    public static ResponseHeader deniedHeader() {
        return new ResponseHeader(StatusCode.REQUEST_DENIED);
    }

    public static ResponseHeader successHeader() {
        return new ResponseHeader(StatusCode.SUCCESS);
    }

    public static ResponseHeader ownerToHeader(String owner) {
        return new ResponseHeader(StatusCode.FOUND).setLocation(owner);
    }

    public static ResponseHeader exceptionToHeader(Throwable t) {
        ResponseHeader response = new ResponseHeader();
        if (t instanceof DLException) {
            DLException dle = (DLException) t;
            if (dle instanceof OwnershipAcquireFailedException) {
                response.setLocation(((OwnershipAcquireFailedException) dle).getCurrentOwner());
            }
            response.setCode(dle.getCode());
            response.setErrMsg(dle.getMessage());
        } else {
            response.setCode(StatusCode.INTERNAL_SERVER_ERROR);
            response.setErrMsg("Internal server error : " + t.getMessage());
        }
        return response;
    }

    public static WriteResponse write(ResponseHeader responseHeader) {
        return new WriteResponse(responseHeader);
    }

    public static WriteResponse writeSuccess() {
        return new WriteResponse(successHeader());
    }

    public static WriteResponse writeDenied() {
        return new WriteResponse(deniedHeader());
    }

    public static BulkWriteResponse bulkWrite(ResponseHeader responseHeader) {
        return new BulkWriteResponse(responseHeader);
    }

    public static BulkWriteResponse bulkWriteSuccess() {
        return new BulkWriteResponse(successHeader());
    }

    public static BulkWriteResponse bulkWriteDenied() {
        return new BulkWriteResponse(deniedHeader());
    }
}
