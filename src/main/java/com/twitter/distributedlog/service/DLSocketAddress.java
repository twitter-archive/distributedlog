package com.twitter.distributedlog.service;

import com.google.common.base.Preconditions;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

public class DLSocketAddress {

    private static final String COLON = ":";

    public static SocketAddress parseSocketAddress(String addr) {
        String[] parts =  addr.split(COLON);
        Preconditions.checkArgument(parts.length == 2);
        String hostname = parts[0];
        int port = Integer.parseInt(parts[1]);
        return new InetSocketAddress(hostname, port);
    }

    public static SocketAddress getSocketAddress(int port) throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), port);
    }
}
