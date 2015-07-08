package com.twitter.distributedlog.service.stream;

public interface WriteOpWithPayload {

    // Return the payload size in bytes
    long getPayloadSize();
}
