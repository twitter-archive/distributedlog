package com.twitter.distributedlog.basic;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.LogRecordSet;
import com.twitter.distributedlog.io.CompressionCodec.Type;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Write multiple record atomically
 */
public class AtomicWriter {

    private final static String HELP = "AtomicWriter <finagle-name> <stream> <message>[,<message>]";

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println(HELP);
            return;
        }

        String finagleNameStr = args[0];
        String streamName = args[1];
        String[] messages = new String[args.length - 2];
        System.arraycopy(args, 2, messages, 0, messages.length);

        DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                .clientId(ClientId.apply("atomic-writer"))
                .name("atomic-writer")
                .thriftmux(true)
                .finagleNameStr(finagleNameStr)
                .build();

        final LogRecordSet.Writer recordSetWriter = LogRecordSet.newWriter(16 * 1024, Type.NONE);
        List<Future<DLSN>> writeFutures = Lists.newArrayListWithExpectedSize(messages.length);
        for (String msg : messages) {
            final String message = msg;
            ByteBuffer msgBuf = ByteBuffer.wrap(msg.getBytes(UTF_8));
            Promise<DLSN> writeFuture = new Promise<DLSN>();
            writeFuture.addEventListener(new FutureEventListener<DLSN>() {
                @Override
                public void onFailure(Throwable cause) {
                    System.out.println("Encountered error on writing data");
                    cause.printStackTrace(System.err);
                    Runtime.getRuntime().exit(0);
                }

                @Override
                public void onSuccess(DLSN dlsn) {
                    System.out.println("Write '" + message + "' as record " + dlsn);
                }
            });
            recordSetWriter.writeRecord(msgBuf, writeFuture);
            writeFutures.add(writeFuture);
        }
        FutureUtils.result(
            client.writeRecordSet(streamName, recordSetWriter)
                .addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onFailure(Throwable cause) {
                        System.out.println("Encountered error on writing data");
                        cause.printStackTrace(System.err);
                        Runtime.getRuntime().exit(0);
                    }

                    @Override
                    public void onSuccess(DLSN dlsn) {
                        recordSetWriter.completeTransmit(
                                dlsn.getLogSegmentSequenceNo(),
                                dlsn.getEntryId(),
                                dlsn.getSlotId());
                    }
                })
        );
        FutureUtils.result(Future.collect(writeFutures));
        client.close();
    }
}
