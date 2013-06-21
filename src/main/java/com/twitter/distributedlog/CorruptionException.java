package com.twitter.distributedlog;

import java.io.IOException;

/**
 *
 * This exception occurs in the case of a gap in the transactions, or a
 * corrupt log file.
 */
public class CorruptionException extends IOException {
    static final long serialVersionUID = -4687802717006172702L;

    public CorruptionException(String reason) {
        super(reason);
    }
}