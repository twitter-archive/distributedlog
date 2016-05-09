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
package com.twitter.distributedlog.config;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DistributedLogConfiguration;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Notes:
 * 1. lastModified granularity is platform dependent, generally 1 sec, so we can't wait 1ms for things to
 * get picked up.
 */
public class TestConfigurationSubscription {
    static final Logger LOG = LoggerFactory.getLogger(TestConfigurationSubscription.class);

    @Test(timeout = 60000)
    public void testReloadConfiguration() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        FileConfigurationBuilder builder = new PropertiesConfigurationBuilder(writer.getFile().toURI().toURL());
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new DistributedLogConfiguration());
        List<FileConfigurationBuilder> fileConfigBuilders = Lists.newArrayList(builder);
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, fileConfigBuilders, executorService, 100, TimeUnit.MILLISECONDS);
        final CountDownLatch reloadLatch = new CountDownLatch(1);
        final AtomicReference<ConcurrentBaseConfiguration> confHolder =
                new AtomicReference<ConcurrentBaseConfiguration>();
        confSub.registerListener(new com.twitter.distributedlog.config.ConfigurationListener() {
            @Override
            public void onReload(ConcurrentBaseConfiguration conf) {
                confHolder.set(conf);
                reloadLatch.countDown();
            }
        });
        assertEquals(null, conf.getProperty("prop1"));

        // add
        writer.setProperty("prop1", "1");
        writer.save();

        // reload the config
        confSub.reload();
        reloadLatch.await();
        assertNotNull(confHolder.get());
        assertTrue(conf == confHolder.get());
        assertEquals("1", conf.getProperty("prop1"));
    }

    @Test(timeout = 60000)
    public void testAddReloadBasicsConfig() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        FileConfigurationBuilder builder = new PropertiesConfigurationBuilder(writer.getFile().toURI().toURL());
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new DistributedLogConfiguration());
        List<FileConfigurationBuilder> fileConfigBuilders = Lists.newArrayList(builder);
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, fileConfigBuilders, executorService, 100, TimeUnit.MILLISECONDS);
        assertEquals(null, conf.getProperty("prop1"));

        // add
        writer.setProperty("prop1", "1");
        writer.save();
        ConfigTestUtil.waitForConfig(conf, "prop1", "1");
        assertEquals("1", conf.getProperty("prop1"));
        Thread.sleep(1000);

        // update
        writer.setProperty("prop1", "2");
        writer.save();
        ConfigTestUtil.waitForConfig(conf, "prop1", "2");
        assertEquals("2", conf.getProperty("prop1"));
        Thread.sleep(1000);

        // remove
        writer.removeProperty("prop1");
        writer.save();
        ConfigTestUtil.waitForConfig(conf, "prop1", null);
        assertEquals(null, conf.getProperty("prop1"));
        executorService.shutdown();
    }

    @Test(timeout = 60000)
    public void testInitialConfigLoad() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        writer.setProperty("prop1", "1");
        writer.setProperty("prop2", "abc");
        writer.setProperty("prop3", "123.0");
        writer.setProperty("prop4", "11132");
        writer.setProperty("prop5", "true");
        writer.save();

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        FileConfigurationBuilder builder = new PropertiesConfigurationBuilder(writer.getFile().toURI().toURL());
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new DistributedLogConfiguration());
        List<FileConfigurationBuilder> fileConfigBuilders = Lists.newArrayList(builder);
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, fileConfigBuilders, executorService, 100, TimeUnit.MILLISECONDS);
        assertEquals(1, conf.getInt("prop1"));
        assertEquals("abc", conf.getString("prop2"));
        assertEquals(123.0, conf.getFloat("prop3"), 0);
        assertEquals(11132, conf.getInt("prop4"));
        assertEquals(true, conf.getBoolean("prop5"));
    }

    @Test(timeout = 60000)
    public void testExceptionInConfigLoad() throws Exception {
        PropertiesWriter writer = new PropertiesWriter();
        writer.setProperty("prop1", "1");
        writer.save();

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        FileConfigurationBuilder builder = new PropertiesConfigurationBuilder(writer.getFile().toURI().toURL());
        ConcurrentConstConfiguration conf = new ConcurrentConstConfiguration(new DistributedLogConfiguration());
        List<FileConfigurationBuilder> fileConfigBuilders = Lists.newArrayList(builder);
        ConfigurationSubscription confSub =
                new ConfigurationSubscription(conf, fileConfigBuilders, executorService, 100, TimeUnit.MILLISECONDS);

        final AtomicInteger count = new AtomicInteger(1);
        conf.addConfigurationListener(new ConfigurationListener() {
            @Override
            public void configurationChanged(ConfigurationEvent event) {
                LOG.info("config changed {}", event);
                // Throw after so we actually see the update anyway.
                if (!event.isBeforeUpdate()) {
                    count.getAndIncrement();
                    throw new RuntimeException("config listener threw and exception");
                }
            }
        });

        int i = 0;
        int initial = 0;
        while (count.get() == initial) {
            writer.setProperty("prop1", Integer.toString(i++));
            writer.save();
            Thread.sleep(100);
        }

        initial = count.get();
        while (count.get() == initial) {
            writer.setProperty("prop1", Integer.toString(i++));
            writer.save();
            Thread.sleep(100);
        }
    }
}
