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
package com.twitter.distributedlog.bk;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for quorums
 */
public class QuorumConfig {

    private static final Logger logger = LoggerFactory.getLogger(QuorumConfig.class);

    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;

    public QuorumConfig(int ensembleSize,
                        int writeQuorumSize,
                        int ackQuorumSize) {
        this.ensembleSize = ensembleSize;
        if (this.ensembleSize < writeQuorumSize) {
            this.writeQuorumSize = this.ensembleSize;
            logger.warn("Setting write quorum size {} greater than ensemble size {}",
                    writeQuorumSize, this.ensembleSize);
        } else {
            this.writeQuorumSize = writeQuorumSize;
        }
        if (this.writeQuorumSize < ackQuorumSize) {
            this.ackQuorumSize = this.writeQuorumSize;
            logger.warn("Setting write ack quorum size {} greater than write quorum size {}",
                    ackQuorumSize, this.writeQuorumSize);
        } else {
            this.ackQuorumSize = ackQuorumSize;
        }
    }

    public int getEnsembleSize() {
        return ensembleSize;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ensembleSize, writeQuorumSize, ackQuorumSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QuorumConfig)) {
            return false;
        }
        QuorumConfig other = (QuorumConfig) obj;
        return ensembleSize == other.ensembleSize
                && writeQuorumSize == other.writeQuorumSize
                && ackQuorumSize == other.ackQuorumSize;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QuorumConfig[ensemble=")
          .append(ensembleSize).append(", write quorum=")
          .append(writeQuorumSize).append(", ack quorum=")
          .append(ackQuorumSize).append("]");
        return sb.toString();
    }
}
