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
package com.twitter.distributedlog.mapreduce;

import com.google.common.collect.Sets;
import com.twitter.distributedlog.LogSegmentMetadata;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Version;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A input split that reads from a log segment.
 */
public class LogSegmentSplit extends InputSplit implements Writable {

    private LogSegmentMetadata logSegmentMetadata;
    private LedgerMetadata ledgerMetadata;

    public LogSegmentSplit() {}

    public LogSegmentSplit(LogSegmentMetadata logSegmentMetadata,
                           LedgerMetadata ledgerMetadata) {
        this.logSegmentMetadata = logSegmentMetadata;
        this.ledgerMetadata = ledgerMetadata;
    }

    public LogSegmentMetadata getMetadata() {
        return logSegmentMetadata;
    }

    public long getLedgerId() {
        return logSegmentMetadata.getLedgerId();
    }

    @Override
    public long getLength()
            throws IOException, InterruptedException {
        return logSegmentMetadata.getRecordCount();
    }

    @Override
    public String[] getLocations()
            throws IOException, InterruptedException {
        Set<String> locations = Sets.newHashSet();
        for (ArrayList<BookieSocketAddress> ensemble : ledgerMetadata.getEnsembles().values()) {
            for (BookieSocketAddress host : ensemble) {
                locations.add(host.getHostName());
            }
        }
        return locations.toArray(new String[locations.size()]);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        String lsMetadataStr = logSegmentMetadata.getFinalisedData();
        dataOutput.writeUTF(lsMetadataStr);
        String lhMetadataStr = new String(ledgerMetadata.serialize(), UTF_8);
        dataOutput.writeUTF(lhMetadataStr);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String lsMetadataStr = dataInput.readUTF();
        logSegmentMetadata = LogSegmentMetadata.parseData("",
                lsMetadataStr.getBytes(UTF_8));
        String lhMetadataStr = dataInput.readUTF();
        ledgerMetadata = LedgerMetadata.parseConfig(lhMetadataStr.getBytes(UTF_8),
                Version.ANY);
    }
}
