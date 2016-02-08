package com.twitter.distributedlog.io;

import java.io.ByteArrayOutputStream;

/**
 * {@link ByteArrayOutputStream} based buffer.
 */
public class Buffer extends ByteArrayOutputStream {
    public Buffer(int initialCapacity) {
        super(initialCapacity);
    }

    public byte[] getData() {
        return buf;
    }
}
