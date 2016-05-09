package com.twitter.distributedlog.messaging;

import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.service.DistributedLogClient;
import com.twitter.distributedlog.service.DistributedLogClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.util.FutureEventListener;
import jline.ConsoleReader;
import jline.Terminal;
import org.apache.commons.lang.StringUtils;

/**
 * Writer write records into multiple streams
 */
public class ConsoleProxyRRMultiWriter {

    private final static String HELP = "ConsoleProxyRRMultiWriter <finagle-name> <stream-1>[,<stream-2>,...,<stream-n>]";
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
        RRMultiWriter<Integer, String> writer = new RRMultiWriter(streamNameList, client);

        // Setup Terminal
        Terminal terminal = Terminal.setupTerminal();
        ConsoleReader reader = new ConsoleReader();
        String line;
        while ((line = reader.readLine(PROMPT_MESSAGE)) != null) {
            writer.write(line)
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

        client.close();
    }

}
