package com.twitter.distributedlog.benchmark;

import com.twitter.distributedlog.benchmark.thrift.Message;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;

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
}
