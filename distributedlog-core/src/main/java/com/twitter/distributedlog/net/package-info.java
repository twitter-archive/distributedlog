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
