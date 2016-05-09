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

import com.google.common.base.Optional;

import com.twitter.distributedlog.net.DNSResolverForRacks;
import com.twitter.distributedlog.net.DNSResolverForRows;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.commons.configuration.StrictConfigurationComparator;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestDistributedLogConfiguration {

    static final class TestDNSResolver implements DNSToSwitchMapping {

        public TestDNSResolver() {}

        @Override
        public List<String> resolve(List<String> list) {
            return list;
        }

        @Override
        public void reloadCachedMappings() {
            // no-op
        }
    }

    @Test(timeout = 20000)
    public void loadStreamConfGoodOverrideAccepted() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        assertEquals(conf.getPeriodicFlushFrequencyMilliSeconds(),
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT);
        assertEquals(conf.getReaderIdleErrorThresholdMillis(),
            DistributedLogConfiguration.BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT);
        DistributedLogConfiguration override = new DistributedLogConfiguration();
        override.setPeriodicFlushFrequencyMilliSeconds(
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT+1);
        override.setReaderIdleErrorThresholdMillis(
            DistributedLogConfiguration.BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT - 1);
        conf.loadStreamConf(Optional.of(override));
        assertEquals(conf.getPeriodicFlushFrequencyMilliSeconds(),
            DistributedLogConfiguration.BKDL_PERIODIC_FLUSH_FREQUENCY_MILLISECONDS_DEFAULT+1);
        assertEquals(conf.getReaderIdleErrorThresholdMillis(),
            DistributedLogConfiguration.BKDL_READER_IDLE_ERROR_THRESHOLD_MILLIS_DEFAULT - 1);
    }

    @SuppressWarnings("deprecation")
    @Test(timeout = 20000)
    public void loadStreamConfBadOverrideIgnored() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        assertEquals(conf.getBKClientWriteTimeout(),
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);
        DistributedLogConfiguration override = new DistributedLogConfiguration();
        override.setBKClientWriteTimeout(
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT+1);
        conf.loadStreamConf(Optional.of(override));
        assertEquals(conf.getBKClientWriteTimeout(),
            DistributedLogConfiguration.BKDL_BKCLIENT_WRITE_TIMEOUT_DEFAULT);
    }

    @Test(timeout = 20000)
    public void loadStreamConfNullOverrides() throws Exception {
        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        DistributedLogConfiguration confClone = (DistributedLogConfiguration)conf.clone();
        Optional<DistributedLogConfiguration> streamConfiguration = Optional.absent();
        conf.loadStreamConf(streamConfiguration);

        StrictConfigurationComparator comp = new StrictConfigurationComparator();
        assertTrue(comp.compare(conf, confClone));
    }

    @Test(timeout = 200000)
    public void getEnsemblePlacementResolverClass() throws Exception {
        DistributedLogConfiguration conf1 = new DistributedLogConfiguration();
        assertEquals(DNSResolverForRacks.class, conf1.getEnsemblePlacementDnsResolverClass());
        DistributedLogConfiguration conf2 = new DistributedLogConfiguration()
                .setRowAwareEnsemblePlacementEnabled(true);
        assertEquals(DNSResolverForRows.class, conf2.getEnsemblePlacementDnsResolverClass());
        DistributedLogConfiguration conf3 = new DistributedLogConfiguration()
                .setRowAwareEnsemblePlacementEnabled(true)
                .setEnsemblePlacementDnsResolverClass(TestDNSResolver.class);
        assertEquals(TestDNSResolver.class, conf3.getEnsemblePlacementDnsResolverClass());
    }
}
