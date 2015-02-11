package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

public class RegionUnavailableException extends DLException {

    private static final long serialVersionUID = 5727337162533143957L;

    public RegionUnavailableException(String msg) {
        super(StatusCode.REGION_UNAVAILABLE, msg);
    }
}
