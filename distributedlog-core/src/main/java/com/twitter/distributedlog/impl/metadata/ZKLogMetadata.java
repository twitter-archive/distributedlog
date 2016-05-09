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
package com.twitter.distributedlog.impl.metadata;

import java.net.URI;

/**
 * Class to represent the layout and metadata of the zookeeper-based log metadata
 */
public class ZKLogMetadata {

    protected static String getLogComponentPath(URI uri, String logName, String logIdentifier, String component) {
        return String.format("%s/%s/%s%s", uri.getPath(), logName, logIdentifier, component);
    }

    /**
     * Get the log root path for a given log.
     *
     * @param uri
     *          namespace to store the log
     * @param logName
     *          name of the log
     * @param logIdentifier
     *          identifier of the log
     * @return log root path
     */
    public static String getLogRootPath(URI uri, String logName, String logIdentifier) {
        return getLogComponentPath(uri, logName, logIdentifier, "");
    }

    /**
     * Get the logsegments root path for a given log.
     *
     * @param uri
     *          namespace to store the log
     * @param logName
     *          name of the log
     * @param logIdentifier
     *          identifier of the log
     * @return logsegments root path
     */
    public static String getLogSegmentsPath(URI uri, String logName, String logIdentifier) {
        return getLogComponentPath(uri, logName, logIdentifier, LOGSEGMENTS_PATH);
    }

    protected static final int LAYOUT_VERSION = -1;
    protected final static String LOGSEGMENTS_PATH = "/ledgers";
    protected final static String VERSION_PATH = "/version";
    // writer znodes
    protected final static String MAX_TXID_PATH = "/maxtxid";
    protected final static String LOCK_PATH = "/lock";
    protected final static String ALLOCATION_PATH = "/allocation";
    // reader znodes
    protected final static String READ_LOCK_PATH = "/readLock";

    protected final URI uri;
    protected final String logName;
    protected final String logIdentifier;

    // Root path of the log
    protected final String logRootPath;
    // Components
    protected final String logSegmentsPath;
    protected final String lockPath;
    protected final String maxTxIdPath;
    protected final String allocationPath;

    /**
     * metadata representation of a log
     *
     * @param uri
     *          namespace to store the log
     * @param logName
     *          name of the log
     * @param logIdentifier
     *          identifier of the log
     */
    protected ZKLogMetadata(URI uri,
                            String logName,
                            String logIdentifier) {
        this.uri = uri;
        this.logName = logName;
        this.logIdentifier = logIdentifier;
        this.logRootPath = getLogRootPath(uri, logName, logIdentifier);
        this.logSegmentsPath = logRootPath + LOGSEGMENTS_PATH;
        this.lockPath = logRootPath + LOCK_PATH;
        this.maxTxIdPath = logRootPath + MAX_TXID_PATH;
        this.allocationPath = logRootPath + ALLOCATION_PATH;
    }

    public URI getUri() {
        return uri;
    }

    public String getLogName() {
        return logName;
    }

    /**
     * Get the root path of the log.
     *
     * @return root path of the log.
     */
    public String getLogRootPath() {
        return logRootPath;
    }

    /**
     * Get the root path for log segments.
     *
     * @return root path for log segments
     */
    public String getLogSegmentsPath() {
        return this.logSegmentsPath;
    }

    /**
     * Get the path for a log segment of the log.
     *
     * @param segmentName
     *          segment name
     * @return path for the log segment
     */
    public String getLogSegmentPath(String segmentName) {
        return this.logSegmentsPath + "/" + segmentName;
    }

    public String getLockPath() {
        return lockPath;
    }

    public String getMaxTxIdPath() {
        return maxTxIdPath;
    }

    public String getAllocationPath() {
        return allocationPath;
    }

    /**
     * Get the fully qualified name of the log.
     *
     * @return fully qualified name
     */
    public String getFullyQualifiedName() {
        return String.format("%s:%s", logName, logIdentifier);
    }

}
