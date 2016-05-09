package com.twitter.distributedlog.basic;

import com.google.common.collect.Lists;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.client.DistributedLogMultiStreamWriter;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.FutureEventListener;
import jline.ConsoleReader;
import jline.Terminal;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Writer write records from console
 */
public class ConsoleProxyMultiWriter {

    private final static String HELP = "ConsoleProxyWriter <finagle-name> <stream-1>[,<stream-2>,...,<stream-n>]";
    private final static String PROMPT_MESSAGE = "[dlog] > ";

    public static void main(String[] args) throws Exception {
        if (2 != args.length) {
            System.out.println(HELP);
            return;
        }

        String finagleNameStr = args[0];
        final String streamList = args[1];

        DistributedLogClient client = DistributedLogClientBuilder.newBuilder()
                .clientId(ClientId.apply("console-proxy-writer"))
                .name("console-proxy-writer")
                .thriftmux(true)
                .finagleNameStr(finagleNameStr)
                .build();
        String[] streamNameList = StringUtils.split(streamList, ',');
        DistributedLogMultiStreamWriter multiStreamWriter = DistributedLogMultiStreamWriter.newBuilder()
                .streams(Lists.newArrayList(streamNameList))
                .bufferSize(0)
                .client(client)
                .flushIntervalMs(0)
                .firstSpeculativeTimeoutMs(10000)
                .maxSpeculativeTimeoutMs(20000)
                .requestTimeoutMs(50000)
                .build();

        // Setup Terminal
        Terminal terminal = Terminal.setupTerminal();
        ConsoleReader reader = new ConsoleReader();
        String line;
        while ((line = reader.readLine(PROMPT_MESSAGE)) != null) {
            multiStreamWriter.write(ByteBuffer.wrap(line.getBytes(UTF_8)))
                    .addEventListener(new FutureEventListener<DLSN>() {
                        @Override
                        public void onFailure(Throwable cause) {
                            System.out.println("Encountered error on writing data");
                            cause.printStackTrace(System.err);
                            Runtime.getRuntime().exit(0);
                        }

                        @Override
                        public void onSuccess(DLSN value) {
                            // done
                        }
                    });
        }

        multiStreamWriter.close();
        client.close();
    }

}
