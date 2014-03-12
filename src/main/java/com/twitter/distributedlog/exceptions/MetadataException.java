package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class MetadataException extends DLException {

    private static final long serialVersionUID = 6683578078046016125L;

    public MetadataException(String msg) {
        super(StatusCode.METADATA_EXCEPTION, msg);
    }

    public MetadataException(String msg, Throwable t) {
        super(StatusCode.METADATA_EXCEPTION, msg, t);
    }
}
