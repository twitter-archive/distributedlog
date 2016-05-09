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
package com.twitter.distributedlog.net;

import org.apache.bookkeeper.net.DNSToSwitchMapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utils about network
 */
public class NetUtils {

    /**
     * Get the dns resolver from class <code>resolverClassName</code> with optional
     * <code>hostRegionOverrides</code>.
     * <p>
     * It would try to load the class with the constructor with <code>hostRegionOverrides</code>.
     * If it fails, it would fall back to load the class with default empty constructor.
     * The interpretion of <code>hostRegionOverrides</code> is up to the implementation.
     *
     * @param resolverCls
     *          resolver class
     * @param hostRegionOverrides
     *          host region overrides
     * @return dns resolver
     */
    public static DNSToSwitchMapping getDNSResolver(Class<? extends DNSToSwitchMapping> resolverCls,
                                                    String hostRegionOverrides) {
        // first try to construct the dns resolver with overrides
        Constructor<? extends DNSToSwitchMapping> constructor;
        Object[] parameters;
        try {
            constructor = resolverCls.getDeclaredConstructor(String.class);
            parameters = new Object[] { hostRegionOverrides };
        } catch (NoSuchMethodException nsme) {
            // no constructor with overrides
            try {
                constructor = resolverCls.getDeclaredConstructor();
                parameters = new Object[0];
            } catch (NoSuchMethodException nsme1) {
                throw new RuntimeException("Unable to find constructor for dns resolver "
                        + resolverCls, nsme1);
            }
        }
        constructor.setAccessible(true);
        try {
            return constructor.newInstance(parameters);
        } catch (InstantiationException ie) {
            throw new RuntimeException("Unable to instantiate dns resolver " + resolverCls, ie);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException("Illegal access to dns resolver " + resolverCls, iae);
        } catch (InvocationTargetException ite) {
            throw new RuntimeException("Unable to construct dns resolver " + resolverCls, ite);
        }
    }

}
