package com.twitter.distributedlog.client.ownership;

import com.google.common.collect.ImmutableMap;
import com.twitter.distributedlog.client.stats.OwnershipStatsLogger;
import com.twitter.finagle.stats.StatsReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client Side Ownership Cache
 */
public class OwnershipCache {

    private static final Logger logger = LoggerFactory.getLogger(OwnershipCache.class);

    private final ConcurrentHashMap<String, SocketAddress> stream2Addresses =
            new ConcurrentHashMap<String, SocketAddress>();
    private final ConcurrentHashMap<SocketAddress, Set<String>> address2Streams =
            new ConcurrentHashMap<SocketAddress, Set<String>>();

    // Stats
    private final OwnershipStatsLogger ownershipStatsLogger;

    public OwnershipCache(StatsReceiver statsReceiver,
                          StatsReceiver streamStatsReceiver) {
        this.ownershipStatsLogger = new OwnershipStatsLogger(statsReceiver, streamStatsReceiver);
    }

    public OwnershipStatsLogger getOwnershipStatsLogger() {
        return ownershipStatsLogger;
    }

    /**
     * Update ownership of <i>stream</i> to <i>addr</i>.
     *
     * @param stream
     *          Stream Name.
     * @param addr
     *          Owner Address.
     * @return true if owner is updated
     */
    public boolean updateOwner(String stream, SocketAddress addr) {
        // update ownership
        SocketAddress oldAddr = stream2Addresses.putIfAbsent(stream, addr);
        if (null != oldAddr && oldAddr.equals(addr)) {
            return true;
        }
        if (null != oldAddr) {
            if (stream2Addresses.replace(stream, oldAddr, addr)) {
                // Store the relevant mappings for this topic and host combination
                logger.info("Storing ownership for stream : {}, old host : {}, new host : {}.",
                        new Object[] { stream, oldAddr, addr });
                StringBuilder sb = new StringBuilder();
                sb.append("Ownership changed '")
                  .append(oldAddr).append("' -> '").append(addr).append("'");
                removeOwnerFromStream(stream, oldAddr, sb.toString());

                // update stats
                ownershipStatsLogger.onRemove(stream);
                ownershipStatsLogger.onAdd(stream);
            } else {
                logger.warn("Ownership of stream : {} has been changed from {} to {} when storing host : {}.",
                        new Object[] { stream, oldAddr, stream2Addresses.get(stream), addr });
                return false;
            }
        } else {
            logger.info("Storing ownership for stream : {}, host : {}.", stream, addr);
            // update stats
            ownershipStatsLogger.onAdd(stream);
        }

        Set<String> streamsForHost = address2Streams.get(addr);
        if (null == streamsForHost) {
            Set<String> newStreamsForHost = new HashSet<String>();
            streamsForHost = address2Streams.putIfAbsent(addr, newStreamsForHost);
            if (null == streamsForHost) {
                streamsForHost = newStreamsForHost;
            }
        }
        synchronized (streamsForHost) {
            // check whether the ownership changed, since it might happend after replace succeed
            if (addr.equals(stream2Addresses.get(stream))) {
                streamsForHost.add(stream);
            }
        }
        return true;
    }

    /**
     * Get the cached owner for stream <code>stream</code>.
     *
     * @param stream
     *          stream to lookup ownership
     * @return owner's address
     */
    public SocketAddress getOwner(String stream) {
        SocketAddress address = stream2Addresses.get(stream);
        if (null == address) {
            ownershipStatsLogger.onMiss(stream);
        } else {
            ownershipStatsLogger.onHit(stream);
        }
        return address;
    }

    /**
     * Remove the owner <code>addr</code> from <code>stream</code> for a given <code>reason</code>.
     *
     * @param stream stream name
     * @param addr owner address
     * @param reason reason to remove ownership
     */
    public void removeOwnerFromStream(String stream, SocketAddress addr, String reason) {
        if (stream2Addresses.remove(stream, addr)) {
            logger.info("Removed stream to host mapping for (stream: {} -> host: {}) : reason = '{}'.",
                    new Object[] { stream, addr, reason });
        }
        Set<String> streamsForHost = address2Streams.get(addr);
        if (null != streamsForHost) {
            synchronized (streamsForHost) {
                if (streamsForHost.remove(stream)) {
                    logger.info("Removed stream ({}) from host {} : reason = '{}'.",
                            new Object[] { stream, addr, reason });
                    if (streamsForHost.isEmpty()) {
                        address2Streams.remove(addr, streamsForHost);
                    }
                    ownershipStatsLogger.onRemove(stream);
                }
            }
        }
    }

    /**
     * Remove all streams from host <code>addr</code>.
     *
     * @param addr
     *          host to remove ownerships
     */
    public void removeAllStreamsFromOwner(SocketAddress addr) {
        logger.info("Remove streams mapping for host {}", addr);
        Set<String> streamsForHost = address2Streams.get(addr);
        if (null != streamsForHost) {
            synchronized (streamsForHost) {
                for (String s : streamsForHost) {
                    if (stream2Addresses.remove(s, addr)) {
                        logger.info("Removing mapping for stream : {} from host : {}", s, addr);
                        ownershipStatsLogger.onRemove(s);
                    }
                }
                address2Streams.remove(addr, streamsForHost);
            }
        }
    }

    /**
     * Get the number cached streams.
     *
     * @return number cached streams.
     */
    public int getNumCachedStreams() {
        return stream2Addresses.size();
    }

    /**
     * Get the stream ownership distribution across proxies.
     *
     * @return stream ownership distribution
     */
    public Map<SocketAddress, Set<String>> getStreamOwnershipDistribution() {
        return ImmutableMap.copyOf(address2Streams);
    }

    /**
     * Get the stream ownership mapping.
     *
     * @return stream ownership mapping.
     */
    public Map<String, SocketAddress> getStreamOwnerMapping() {
        return stream2Addresses;
    }

}
