package com.twitter.distributedlog.service;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.distributedlog.thrift.service.StatusCode;
import com.twitter.finagle.ChannelException;
import com.twitter.finagle.NoBrokersAvailableException;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;
import org.apache.commons.lang3.tuple.Pair;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class ConsistentHashRoutingService extends ServerSetRoutingService {

    @Deprecated
    public static ConsistentHashRoutingService of(DLServerSetWatcher serverSetWatcher, int numReplicas) {
        return new ConsistentHashRoutingService(serverSetWatcher, numReplicas, 300);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements RoutingService.Builder {

        private ServerSet _serverSet;
        private boolean _resolveFromName = false;
        private int _numReplicas;
        private int _blackoutSeconds = 300;

        private Builder() {}

        public Builder serverSet(ServerSet serverSet) {
            this._serverSet = serverSet;
            return this;
        }

        public Builder resolveFromName(boolean enabled) {
            this._resolveFromName = enabled;
            return this;
        }

        public Builder numReplicas(int numReplicas) {
            this._numReplicas = numReplicas;
            return this;
        }

        public Builder blackoutSeconds(int seconds) {
            this._blackoutSeconds = seconds;
            return this;
        }

        @Override
        public RoutingService build() {
            Preconditions.checkNotNull(_serverSet, "No serverset provided.");
            Preconditions.checkArgument(_numReplicas > 0, "Invalid number of replicas : " + _numReplicas);
            return new ConsistentHashRoutingService(new DLServerSetWatcher(_serverSet, _resolveFromName),
                    _numReplicas, _blackoutSeconds);
        }
    }

    static class ConsistentHash {
        private final HashFunction hashFunction;
        private final int numOfReplicas;
        private final SortedMap<Long, SocketAddress> circle;

        ConsistentHash(HashFunction hashFunction, int numOfReplicas) {
            this.hashFunction = hashFunction;
            this.numOfReplicas = numOfReplicas;
            this.circle = new TreeMap<Long, SocketAddress>();
        }

        private String replicaName(int shardId, int replica, String address) {
            if (shardId < 0) {
                shardId = UNKNOWN_SHARD_ID;
            }

            StringBuilder sb = new StringBuilder(100);
            sb.append("shard-");
            sb.append(shardId);
            sb.append('-');
            sb.append(replica);
            sb.append('-');
            sb.append(address);

            return sb.toString();
        }

        private Long replicaHash(int shardId, int replica, String address) {
            return hashFunction.hashUnencodedChars(replicaName(shardId, replica, address)).asLong();
        }

        private Long replicaHash(int shardId, int replica, SocketAddress address) {
            return replicaHash(shardId, replica, address.toString());
        }

        public synchronized void add(int shardId, SocketAddress address) {
            String addressStr = address.toString();
            for (int i = 0; i < numOfReplicas; i++) {
                Long hash = replicaHash(shardId, i, addressStr);
                circle.put(hash, address);
            }
        }

        public synchronized void remove(int shardId, SocketAddress address) {
            for (int i = 0; i < numOfReplicas; i++) {
                long hash = replicaHash(shardId, i, address);
                SocketAddress oldAddress = circle.get(hash);
                if (null != oldAddress && oldAddress.equals(address)) {
                    circle.remove(hash);
                }
            }
        }

        public SocketAddress get(String key, SocketAddress prevAddr) {
            long hash = hashFunction.hashUnencodedChars(key).asLong();
            Pair<Long, SocketAddress> pair = get(hash);
            if (null == pair) {
                return null;
            } else {
                if (pair.getRight().equals(prevAddr)) {
                    Pair<Long, SocketAddress> pair2 = get(pair.getLeft() + 1);
                    if (null == pair2) {
                        return null;
                    } else {
                        return pair2.getRight();
                    }
                } else {
                    return pair.getRight();
                }
            }
        }

        private synchronized Pair<Long, SocketAddress> get(long hash) {
            if (circle.isEmpty()) {
                return null;
            }

            if (!circle.containsKey(hash)) {
                SortedMap<Long, SocketAddress> tailMap = circle.tailMap(hash);
                hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            }
            return Pair.of(hash, circle.get(hash));
        }

        synchronized void dumpHashRing() {
            for (Map.Entry<Long, SocketAddress> entry : circle.entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }
        }

    }

    class BlackoutHost implements TimerTask {
        final int shardId;
        final SocketAddress address;

        BlackoutHost(int shardId, SocketAddress address) {
            this.shardId = shardId;
            this.address = address;
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (!timeout.isExpired()) {
                return;
            }
            Set<SocketAddress> removedList = new HashSet<SocketAddress>();
            // add the shard back
            synchronized (shardId2Address) {
                SocketAddress curHost = shardId2Address.get(shardId);
                if (null != curHost) {
                    // there is already new shard joint, so drop the host.
                    logger.info("Blackout Shard {} ({}) was already replaced by {} permanently.",
                            new Object[] { shardId, address, curHost });
                    return;
                }
                join(shardId, address, removedList);
            }
            for (RoutingListener listener : listeners) {
                listener.onServerJoin(address);
            }
        }
    }

    protected final HashedWheelTimer hashedWheelTimer;
    protected final HashFunction hashFunction = Hashing.md5();
    protected final ConsistentHash circle;
    protected final Map<Integer, SocketAddress> shardId2Address =
            new HashMap<Integer, SocketAddress>();
    protected final Map<SocketAddress, Integer> address2ShardId =
            new HashMap<SocketAddress, Integer>();

    // blackout period
    protected final int blackoutSeconds;

    private static final int UNKNOWN_SHARD_ID = -1;

    private ConsistentHashRoutingService(DLServerSetWatcher serverSetWatcher,
                                         int numReplicas,
                                         int blackoutSeconds) {
        super(serverSetWatcher);
        this.circle = new ConsistentHash(hashFunction, numReplicas);
        this.hashedWheelTimer = new HashedWheelTimer(new ThreadFactoryBuilder()
                .setNameFormat("ConsistentHashRoutingService-Timer-%d").build());
        this.blackoutSeconds = blackoutSeconds;
    }

    @Override
    public void startService() {
        super.startService();
        this.hashedWheelTimer.start();
    }

    @Override
    public void stopService() {
        this.hashedWheelTimer.stop();
        super.stopService();
    }

    @Override
    public SocketAddress getHost(String key, SocketAddress previousAddr) throws NoBrokersAvailableException {
        return getHost(key, previousAddr, StatusCode.FOUND);
    }

    @Override
    public SocketAddress getHost(String key, SocketAddress previousAddr, StatusCode previousCode)
            throws NoBrokersAvailableException {
        SocketAddress host = circle.get(key, previousAddr);
        if (null != host) {
            return host;
        }
        throw new NoBrokersAvailableException("No host found for " + key + ", previous : " + previousAddr);
    }

    @Override
    public void removeHost(SocketAddress host, Throwable reason) {
        removeHostInternal(host, Optional.of(reason));
    }

    private void removeHostInternal(SocketAddress host, Optional<Throwable> reason) {
        synchronized (shardId2Address) {
            Integer shardId = address2ShardId.remove(host);
            if (null != shardId) {
                SocketAddress curHost = shardId2Address.get(shardId);
                if (null != curHost && curHost.equals(host)) {
                    shardId2Address.remove(shardId);
                }
                circle.remove(shardId, host);
                if (reason.isPresent()) {
                    if (reason.get() instanceof ChannelException) {
                        logger.info("Shard {} ({}) left due to exception, black it out for {} seconds",
                                new Object[] { shardId, host, blackoutSeconds, reason.get() });
                        BlackoutHost blackoutHost = new BlackoutHost(shardId, host);
                        hashedWheelTimer.newTimeout(blackoutHost, blackoutSeconds, TimeUnit.SECONDS);
                    } else {
                        logger.info("Shard {} ({}) left due to exception",
                                new Object[] { shardId, host, reason.get() });
                    }
                } else {
                    logger.info("Shard {} ({}) left after server set change",
                                shardId, host);
                }
            } else if (reason.isPresent()) {
                logger.info("Node {} left due to to exception ", host, reason.get());
            } else {
                logger.info("Node {} left after server set change", host);
            }
        }
    }

    /**
     * The caller should synchronize on <i>shardId2Address</i>.
     * @param shardId
     *          Shard id of new host joined.
     * @param newHost
     *          New host joined.
     * @param removedList
     *          Old hosts to remove
     */
    private void join(int shardId, SocketAddress newHost, Set<SocketAddress> removedList) {
        SocketAddress oldHost = shardId2Address.put(shardId, newHost);
        if (null != oldHost) {
            // remove the old host only when a new shard is kicked in to replace it.
            address2ShardId.remove(oldHost);
            circle.remove(shardId, oldHost);
            removedList.add(oldHost);
            logger.info("Shard {} ({}) left permanently.", shardId, oldHost);
        }
        address2ShardId.put(newHost, shardId);
        circle.add(shardId, newHost);
        logger.info("Shard {} ({}) joined to replace ({}).",
                    new Object[] { shardId, newHost, oldHost });
    }

    @Override
    protected synchronized void performServerSetChange(ImmutableSet<ServiceInstance> serviceInstances, boolean resolvedFromName) {
        Set<SocketAddress> joinedList = new HashSet<SocketAddress>();
        Set<SocketAddress> removedList = new HashSet<SocketAddress>();
        if (resolvedFromName) {
            Set<SocketAddress> newSet = new HashSet<SocketAddress>();
            for (ServiceInstance serviceInstance : serviceInstances) {
                Endpoint endpoint = serviceInstance.getAdditionalEndpoints().get("thrift");
                SocketAddress address = new InetSocketAddress(endpoint.getHost(), endpoint.getPort());
                newSet.add(address);
            }

            synchronized (shardId2Address) {
                removedList = Sets.difference(address2ShardId.keySet(), newSet).immutableCopy();
                joinedList = Sets.difference(newSet, address2ShardId.keySet()).immutableCopy();
                for (SocketAddress node: removedList) {
                    removeHostInternal(node, Optional.<Throwable>absent());
                }
                for (SocketAddress node: joinedList) {
                    // Assign a random negative shardId
                    int shardId;
                    do {
                        shardId = Math.min(-1 , (int)(Math.random() * Integer.MIN_VALUE));
                    } while (null != shardId2Address.get(shardId));

                    // Since we have allocated a unique id, we don't expect to ever
                    // have to populate anything in the removedList; Since its an immutable set;
                    // add will throw and be a good check in case there are cases when we
                    // don't maintain the set correctly
                    join(shardId, node, removedList);
                }
            }
        } else {
            Map<Integer, SocketAddress> newMap = new HashMap<Integer, SocketAddress>();
            for (ServiceInstance serviceInstance : serviceInstances) {
                Endpoint endpoint = serviceInstance.getAdditionalEndpoints().get("thrift");
                SocketAddress address = new InetSocketAddress(endpoint.getHost(), endpoint.getPort());
                newMap.put(serviceInstance.getShard(), address);
            }

            Map<Integer, SocketAddress> left;
            synchronized (shardId2Address) {
                MapDifference<Integer, SocketAddress> difference =
                        Maps.difference(shardId2Address, newMap);
                left = difference.entriesOnlyOnLeft();
                for (Integer shard : left.keySet()) {
                    SocketAddress host = shardId2Address.get(shard);
                    if (null != host) {
                        // we don't remove those hosts that just disappered on serverset proactively,
                        // since it might be just because serverset become flaky
                        // address2ShardId.remove(host);
                        // circle.remove(shard, host);
                        logger.info("Shard {} ({}) left temporarily.", shard, host);
                    }
                }
                // we need to find if any shards are replacing old shards
                for (Integer shard : newMap.keySet()) {
                    SocketAddress oldHost = shardId2Address.get(shard);
                    SocketAddress newHost = newMap.get(shard);
                    if (!newHost.equals(oldHost)) {
                        join(shard, newHost, removedList);
                        joinedList.add(newHost);
                    }
                }
            }
        }

        for (SocketAddress addr : removedList) {
            for (RoutingListener listener : listeners) {
                listener.onServerLeft(addr);
            }
        }

        for (SocketAddress addr : joinedList) {
            for (RoutingListener listener : listeners) {
                listener.onServerJoin(addr);
            }
        }
    }

    public static void main(String[] args) {
        ConsistentHash circle = new ConsistentHash(Hashing.md5(), 997);

        int numNodes = 40;
        int numStreams = 8000;

        for (int i = 0; i < numNodes; i++) {
            circle.add(i, new InetSocketAddress("localhost", 8000 + i));
        }

        circle.dumpHashRing();

        Map<SocketAddress, Set<String>> addr2Streams =
                new HashMap<SocketAddress, Set<String>>();
        for (int i = 0; i < numNodes; i++) {
            for (int j = 0; j < numStreams / numNodes; j++) {
                String stream = "QuantumLeapX-" + i + "-" + j;
                SocketAddress address = circle.get(stream, null);
                Set<String> streams = addr2Streams.get(address);
                if (null == streams) {
                    streams = new HashSet<String>();
                    addr2Streams.put(address, streams);
                }
                streams.add(stream);
            }
        }

        for (Map.Entry<SocketAddress, Set<String>> entry : addr2Streams.entrySet()) {
            System.out.println(entry.getKey() + " : size = " + entry.getValue().size() + ", -> " + entry.getValue());
        }

        circle.remove(0, new InetSocketAddress("localhost", 8000));
        // circle.add(0, new InetSocketAddress("localhost", 9000));
        Map<SocketAddress, Set<String>> addr2Streams2 =
                new HashMap<SocketAddress, Set<String>>();
        for (int i = 0; i < numNodes; i++) {
            for (int j = 0; j < numStreams / numNodes; j++) {
                String stream = "QuantumLeapX-" + i + "-" + j;
                SocketAddress address = circle.get(stream, null);
                Set<String> streams = addr2Streams2.get(address);
                if (null == streams) {
                    streams = new HashSet<String>();
                    addr2Streams2.put(address, streams);
                }
                streams.add(stream);
            }
        }
        diff(addr2Streams, addr2Streams2);

        for (Map.Entry<SocketAddress, Set<String>> entry : addr2Streams2.entrySet()) {
            System.out.println(entry.getKey() + " : size = " + entry.getValue().size() + ", -> " + entry.getValue());
        }
    }

    private static void diff(Map<SocketAddress, Set<String>> map1, Map<SocketAddress, Set<String>> map2) {
        Set<SocketAddress> ks1 = map1.keySet();
        Set<SocketAddress> ks2 = map2.keySet();
        Set<SocketAddress> diff1 = Sets.difference(ks1, ks2).immutableCopy();
        System.out.println("Map1 - Map2");
        for (SocketAddress addr : diff1) {
            Set<String> streams = map1.get(addr);
            System.out.println(addr + " : size = " + streams.size() + ", -> " + streams);
        }
        Set<SocketAddress> diff2 = Sets.difference(ks2, ks1).immutableCopy();
        System.out.println("Map2 - Map1");
        for (SocketAddress addr : diff2) {
            Set<String> streams = map2.get(addr);
            System.out.println(addr + " : size = " + streams.size() + ", -> " + streams);
        }
        Set<SocketAddress> diff3 = Sets.intersection(ks1, ks2).immutableCopy();
        for (SocketAddress addr : diff3) {
            Set<String> ss1 = map1.get(addr);
            Set<String> ss2 = map2.get(addr);
            System.out.println(addr + " : diff " + Sets.symmetricDifference(ss1, ss2));
        }
    }

}
