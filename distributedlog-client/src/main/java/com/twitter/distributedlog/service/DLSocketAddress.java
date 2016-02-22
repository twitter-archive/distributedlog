package com.twitter.distributedlog.service;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class DLSocketAddress {

    private static final int VERSION = 1;

    private static final String COLON = ":";
    private static final String SEP = ";";

    private final int shard;
    private final InetSocketAddress socketAddress;

    public DLSocketAddress(int shard, InetSocketAddress socketAddress) {
        this.shard = shard;
        this.socketAddress = socketAddress;
    }

    /**
     * Shard id for dl write proxy.
     *
     * @return shard id for dl write proxy.
     */
    public int getShard() {
        return shard;
    }

    /**
     * Socket address for dl write proxy
     *
     * @return socket address for dl write proxy
     */
    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    /**
     * Serialize the write proxy identifier to string.
     *
     * @return serialized write proxy identifier.
     */
    public String serialize() {
        return toLockId(socketAddress, shard);
    }

    @Override
    public int hashCode() {
        return socketAddress.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DLSocketAddress)) {
            return false;
        }
        DLSocketAddress other = (DLSocketAddress) obj;
        return shard == other.shard && socketAddress.equals(other.socketAddress);
    }

    @Override
    public String toString() {
        return toLockId(socketAddress, shard);
    }

    /**
     * Deserialize proxy address from a string representation.
     *
     * @param lockId
     *          string representation of the proxy address.
     * @return proxy address.
     * @throws IOException
     */
    public static DLSocketAddress deserialize(String lockId) throws IOException {
        String parts[] = lockId.split(SEP);
        if (3 != parts.length) {
            throw new IOException("Invalid dl socket address " + lockId);
        }
        int version;
        try {
            version = Integer.parseInt(parts[0]);
        } catch (NumberFormatException nfe) {
            throw new IOException("Invalid version found in " + lockId, nfe);
        }
        if (VERSION != version) {
            throw new IOException("Invalid version " + version + " found in " + lockId + ", expected " + VERSION);
        }
        int shardId;
        try {
            shardId = Integer.parseInt(parts[1]);
        } catch (NumberFormatException nfe) {
            throw new IOException("Invalid shard id found in " + lockId, nfe);
        }
        InetSocketAddress address = parseSocketAddress(parts[2]);
        return new DLSocketAddress(shardId, address);
    }

    /**
     * Parse the inet socket address from the string representation.
     *
     * @param addr
     *          string representation
     * @return inet socket address
     */
    public static InetSocketAddress parseSocketAddress(String addr) {
        String[] parts =  addr.split(COLON);
        Preconditions.checkArgument(parts.length == 2);
        String hostname = parts[0];
        int port = Integer.parseInt(parts[1]);
        return new InetSocketAddress(hostname, port);
    }

    public static InetSocketAddress getSocketAddress(int port) throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), port);
    }

    /**
     * Convert inet socket address to the string representation.
     *
     * @param address
     *          inet socket address.
     * @return string representation of inet socket address.
     */
    public static String toString(InetSocketAddress address) {
        StringBuilder sb = new StringBuilder();
        sb.append(address.getHostName()).append(COLON).append(address.getPort());
        return sb.toString();
    }

    public static String toLockId(InetSocketAddress address, int shard) {
        StringBuilder sb = new StringBuilder();
        sb.append(VERSION).append(SEP).append(shard).append(SEP).append(toString(address));
        return sb.toString();
    }

}
