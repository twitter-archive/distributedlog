/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
