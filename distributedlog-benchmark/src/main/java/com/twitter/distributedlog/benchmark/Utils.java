package com.twitter.distributedlog.benchmark;

import com.google.common.base.Preconditions;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common_internal.zookeeper.TwitterServerSet;
import com.twitter.common_internal.zookeeper.TwitterZk;
import com.twitter.distributedlog.benchmark.thrift.Message;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;

public class Utils {

    static final Random random = new Random(System.currentTimeMillis());
    static final ThreadLocal<TSerializer> MSG_SERIALIZER =
            new ThreadLocal<TSerializer>() {
                @Override
                public TSerializer initialValue() {
                    return new TSerializer(new TBinaryProtocol.Factory());
                }
            };

    public static byte[] generateMessage(long requestMillis, int payLoadSize) throws TException {
        byte[] payload = new byte[payLoadSize];
        random.nextBytes(payload);
        Message msg = new Message(requestMillis, ByteBuffer.wrap(payload));
        return MSG_SERIALIZER.get().serialize(msg);
    }

    public static Message parseMessage(byte[] data) throws TException {
        Message msg = new Message();
        TMemoryInputTransport transport = new TMemoryInputTransport(data);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        msg.read(protocol);
        return msg;
    }

    public static Iterable<InetSocketAddress> getSdZkEndpointsForDC(String dc) {
        if ("atla".equals(dc)) {
            return TwitterZk.ATLA_SD_ZK_ENDPOINTS;
        } else if ("smf1".equals(dc)) {
            return TwitterZk.SMF1_SD_ZK_ENDPOINTS;
        } else {
            return TwitterZk.SD_ZK_ENDPOINTS;
        }
    }

    public static Pair<ZooKeeperClient, ServerSet> parseServerSet(String serverSetPath) {
        String[] serverSetParts = StringUtils.split(serverSetPath, '/');
        Preconditions.checkArgument(serverSetParts.length == 3 || serverSetParts.length == 4,
                "serverset path is malformed: must be role/env/job or dc/role/env/job");
        TwitterServerSet.Service zkService;
        Iterable<InetSocketAddress> zkEndPoints;
        if (serverSetParts.length == 3) {
            zkEndPoints = TwitterZk.SD_ZK_ENDPOINTS;
            zkService = new TwitterServerSet.Service(serverSetParts[0], serverSetParts[1], serverSetParts[2]);
        } else {
            zkEndPoints = Utils.getSdZkEndpointsForDC(serverSetParts[0]);
            zkService = new TwitterServerSet.Service(serverSetParts[1], serverSetParts[2], serverSetParts[3]);
        }
        ZooKeeperClient zkClient =
                TwitterServerSet.clientBuilder(zkService).zkEndpoints(zkEndPoints).build();
        ServerSet serverSet = TwitterServerSet.create(zkClient, zkService);
        return Pair.of(zkClient, serverSet);
    }

}
