package com.twitter.distributedlog.service;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

public class DLSocketAddress {

    private static final int VERSION = 1;

    private static final String COLON = ":";
    private static final String SEP = ";";

    private final int shard;
    private final InetSocketAddress socketAddress;

    DLSocketAddress(int shard, InetSocketAddress socketAddress) {
        this.shard = shard;
        this.socketAddress = socketAddress;
    }

    public int getShard() {
        return shard;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

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
