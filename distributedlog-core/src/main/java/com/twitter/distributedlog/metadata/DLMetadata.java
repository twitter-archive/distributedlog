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
package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.util.Utils;
import com.twitter.distributedlog.ZooKeeperClient;
import com.twitter.distributedlog.ZooKeeperClientBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Metadata of a given DL instance.
 */
public class DLMetadata {

    static final Logger LOG = LoggerFactory.getLogger(DLMetadata.class);

    static final String LINE_SPLITTER = "\n";
    static final String BK_DL_TYPE = "BKDL";
    static final int METADATA_FORMAT_VERSION = 1;

    // metadata format version
    private int metadataFormatVersion = 0;
    // underlying dl type
    private String dlType;
    // underlying dl config
    private DLConfig dlConfig;

    public DLMetadata(String dlType, DLConfig dlConfig) {
        this(dlType, dlConfig, METADATA_FORMAT_VERSION);
    }

    DLMetadata(String dlType, DLConfig dlConfig, int metadataFormatVersion) {
        this.dlType = dlType;
        this.dlConfig = dlConfig;
        this.metadataFormatVersion = metadataFormatVersion;
    }

    /**
     * @return DL type
     */
    public String getDLType() {
        return dlType;
    }

    /**
     * @return DL Config
     */
    public DLConfig getDLConfig() {
        return dlConfig;
    }

    /**
     * Serialize the DL metadata into bytes array.
     *
     * @return bytes of DL metadata.
     */
    public byte[] serialize() {
        StringBuilder sb = new StringBuilder();
        sb.append(metadataFormatVersion).append(LINE_SPLITTER);
        sb.append(dlType).append(LINE_SPLITTER);
        sb.append(dlConfig.serialize());
        LOG.debug("Serialized dl metadata {}.", sb);
        return sb.toString().getBytes(UTF_8);
    }

    @Override
    public int hashCode() {
        return dlType.hashCode() * 13 + dlConfig.hashCode();
    }

    @Override
    public String toString() {
        return new String(serialize(), UTF_8);
    }

    public void update(URI uri) throws IOException {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .requestRateLimit(conf.getZKRequestRateLimit())
                .zkAclId(conf.getZkAclId())
                .uri(uri)
                .build();
        byte[] data = serialize();
        try {
            zkc.get().setData(uri.getPath(), data, -1);
        } catch (KeeperException e) {
            throw new IOException("Fail to update dl metadata " + new String(data, UTF_8)
                    + " to uri " + uri, e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when updating dl metadata "
                    + new String(data, UTF_8) + " to uri " + uri, e);
        } finally {
            zkc.close();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DLMetadata)) {
            return false;
        }
        DLMetadata other = (DLMetadata) o;
        return dlType.equals(other.dlType) && dlConfig.equals(other.dlConfig);
    }

    public void create(URI uri) throws IOException {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .requestRateLimit(conf.getZKRequestRateLimit())
                .zkAclId(conf.getZkAclId())
                .uri(uri)
                .build();
        byte[] data = serialize();
        try {
            Utils.zkCreateFullPathOptimistic(zkc, uri.getPath(), data,
                    zkc.getDefaultACL(), CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new IOException("Fail to write dl metadata " + new String(data, UTF_8)
                    +  " to uri " + uri, e);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when writing dl metadata " + new String(data, UTF_8)
                    + " to uri " + uri, e);
        } finally {
            zkc.close();
        }
    }

    public static void unbind(URI uri) throws IOException {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        ZooKeeperClient zkc = ZooKeeperClientBuilder.newBuilder()
                .sessionTimeoutMs(conf.getZKSessionTimeoutMilliseconds())
                .retryThreadCount(conf.getZKClientNumberRetryThreads())
                .requestRateLimit(conf.getZKRequestRateLimit())
                .zkAclId(conf.getZkAclId())
                .uri(uri)
                .build();
        byte[] data = new byte[0];
        try {
            zkc.get().setData(uri.getPath(), data, -1);
        } catch (KeeperException ke) {
            throw new IOException("Fail to unbound dl metadata on uri " + uri, ke);
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted when unbinding dl metadata on uri " + uri, ie);
        } finally {
            zkc.close();
        }
    }

    /**
     * Deserialize dl metadata of given <i>uri</i> from a given bytes array.
     *
     * @param uri
     *          uri that stored dl metadata bindings
     * @param data
     *          bytes of dl metadata
     * @return dl metadata
     * @throws IOException if failed to parse the bytes array
     */
    public static DLMetadata deserialize(URI uri, byte[] data) throws IOException {
        String metadata = new String(data, UTF_8);
        LOG.debug("Parsing dl metadata {}.", metadata);
        BufferedReader br = new BufferedReader(new StringReader(metadata));
        String versionLine = br.readLine();
        if (null == versionLine) {
            throw new IOException("Empty DL Metadata.");
        }
        int version;
        try {
            version = Integer.parseInt(versionLine);
        } catch (NumberFormatException nfe) {
            version = -1;
        }
        if (METADATA_FORMAT_VERSION != version) {
            throw new IOException("Metadata version not compatible. Expected "
                    + METADATA_FORMAT_VERSION + " but got " + version);
        }
        String type = br.readLine();
        if (!BK_DL_TYPE.equals(type)) {
            throw new IOException("Invalid DL type : " + type);
        }
        DLConfig dlConfig = new BKDLConfig(uri);
        StringBuilder sb = new StringBuilder();
        String line;
        while (null != (line = br.readLine())) {
            sb.append(line);
        }
        dlConfig.deserialize(sb.toString().getBytes(UTF_8));
        return new DLMetadata(type, dlConfig, version);
    }

    public static DLMetadata create(BKDLConfig bkdlConfig) {
        return new DLMetadata(BK_DL_TYPE, bkdlConfig);
    }

}

