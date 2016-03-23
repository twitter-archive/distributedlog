package com.twitter.distributedlog;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import com.twitter.distributedlog.io.Buffer;
import com.twitter.distributedlog.io.CompressionCodec;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestEnvelopedEntry {

    static final Logger LOG = LoggerFactory.getLogger(TestEnvelopedEntry.class);

    private String getString(boolean compressible) {
        if (compressible) {
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i < 1000; i++) {
                builder.append('A');
            }
            return builder.toString();
        }
        return "DistributedLogEnvelopedEntry";
    }

    @Test(timeout = 20000)
    public void testEnvelope() throws Exception {
        byte[] data = getString(false).getBytes();
        EnvelopedEntry writeEntry = new EnvelopedEntry(EnvelopedEntry.CURRENT_VERSION,
                                                  CompressionCodec.Type.NONE,
                                                  data,
                                                  data.length,
                                                  new NullStatsLogger());
        Buffer outBuf = new Buffer(2 * data.length);
        writeEntry.writeFully(new DataOutputStream(outBuf));
        EnvelopedEntry readEntry = new EnvelopedEntry(EnvelopedEntry.CURRENT_VERSION,
                                                      new NullStatsLogger());
        readEntry.readFully(new DataInputStream(new ByteArrayInputStream(outBuf.getData())));
        byte[] newData = readEntry.getDecompressedPayload();
        Assert.assertEquals("Written data should equal read data", new String(data), new String(newData));
    }

    @Test(timeout = 20000)
    public void testLZ4Compression() throws Exception {
        byte[] data = getString(true).getBytes();
        EnvelopedEntry writeEntry = new EnvelopedEntry(EnvelopedEntry.CURRENT_VERSION,
                                                       CompressionCodec.Type.LZ4,
                                                       data,
                                                       data.length,
                                                       new NullStatsLogger());
        Buffer outBuf = new Buffer(data.length);
        writeEntry.writeFully(new DataOutputStream(outBuf));
        Assert.assertTrue(data.length > outBuf.size());
        EnvelopedEntry readEntry = new EnvelopedEntry(EnvelopedEntry.CURRENT_VERSION,
                                                      new NullStatsLogger());
        readEntry.readFully(new DataInputStream(new ByteArrayInputStream(outBuf.getData())));
        byte[] newData = readEntry.getDecompressedPayload();
        Assert.assertEquals("Written data should equal read data", new String(data), new String(newData));
    }
}
