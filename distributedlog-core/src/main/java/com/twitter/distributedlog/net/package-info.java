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
/**
 * This package contains all the utilities of network.
 *
 * <h2>DNSResolver</h2>
 *
 * DNS resolver is the utility to resolve host name to a string which represents this host's network location.
 * BookKeeper will use such network locations to place ensemble to ensure rack or region diversity to ensure
 * data availability in the case of switch/router/region is down.
 * <p>
 * Available dns resolvers:
 * <ul>
 * <li>{@link com.twitter.distributedlog.net.DNSResolverForRacks}
 * <li>{@link com.twitter.distributedlog.net.DNSResolverForRows}
 * </ul>
 */
package com.twitter.distributedlog.net;
