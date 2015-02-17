package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;

/**
 * Exception thrown when encounter invalid enveloped entry
 */
public class InvalidEnvelopedEntryException extends DLException {

    private static final long serialVersionUID = -9190621788978573862L;

    public InvalidEnvelopedEntryException(String msg) {
        super(StatusCode.INVALID_ENVELOPED_ENTRY, msg);
    }
}
