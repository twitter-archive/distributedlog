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
package com.twitter.distributedlog.client.resolver;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultRegionResolver implements RegionResolver {

    private static final String DEFAULT_REGION = "default-region";

    private final Map<SocketAddress, String> regionOverrides =
            new HashMap<SocketAddress, String>();
    private final ConcurrentMap<SocketAddress, String> regionMap =
            new ConcurrentHashMap<SocketAddress, String>();

    public DefaultRegionResolver() {
    }

    public DefaultRegionResolver(Map<SocketAddress, String> regionOverrides) {
        this.regionOverrides.putAll(regionOverrides);
    }

    @Override
    public String resolveRegion(SocketAddress address) {
        String region = regionMap.get(address);
        if (null == region) {
            region = doResolveRegion(address);
            regionMap.put(address, region);
        }
        return region;
    }

    private String doResolveRegion(SocketAddress address) {
        String region = regionOverrides.get(address);
        if (null != region) {
            return region;
        }

        String domainName;
        if (address instanceof InetSocketAddress) {
            InetSocketAddress iAddr = (InetSocketAddress) address;
            domainName = iAddr.getHostName();
        } else {
            domainName = address.toString();
        }
        String[] parts = domainName.split("\\.");
        if (parts.length <= 0) {
            return DEFAULT_REGION;
        }
        String hostName = parts[0];
        String[] labels = hostName.split("-");
        if (labels.length != 4) {
            return DEFAULT_REGION;
        }
        return labels[0];
    }

    @Override
    public void removeCachedHost(SocketAddress address) {
        regionMap.remove(address);
    }
}
