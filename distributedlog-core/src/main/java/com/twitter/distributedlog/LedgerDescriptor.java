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

public class LedgerDescriptor {
    private final long ledgerId;
    private final long logSegmentSequenceNo;
    private final boolean fenced;

    public LedgerDescriptor(long ledgerId, long logSegmentSequenceNo, boolean fenced) {
        this.ledgerId = ledgerId;
        this.logSegmentSequenceNo = logSegmentSequenceNo;
        this.fenced = fenced;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getLogSegmentSequenceNo() {
        return logSegmentSequenceNo;
    }

    public boolean isFenced() {
        return fenced;
    }

    // Only compares the key portion
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LedgerDescriptor)) {
            return false;
        }
        LedgerDescriptor key = (LedgerDescriptor) other;
        return ledgerId == key.ledgerId &&
            fenced == key.fenced;
    }

    @Override
    public int hashCode() {
        return (int) (ledgerId * 13 ^ (fenced ? 0xFFFF : 0xF0F0) * 17);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(lid=").append(ledgerId).append(", lseqno=").append(logSegmentSequenceNo)
                .append(", fenced=").append(fenced).append(")");
        return sb.toString();
    }
}

