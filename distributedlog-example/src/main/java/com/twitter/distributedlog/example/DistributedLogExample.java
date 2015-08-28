package com.twitter.distributedlog.example;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogWriter;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;

import java.net.URI;

import static com.google.common.base.Charsets.UTF_8;

public class DistributedLogExample {

    private static byte[] generatePayload(String prefix, long txn) {
        return String.format("%s-%d", prefix, txn).getBytes(UTF_8);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: DistributedLogExample <uri>");
            System.exit(-1);
        }
        URI uri = URI.create(args[0]);
        // Create a distributedlog configuration
        DistributedLogConfiguration conf =
            new DistributedLogConfiguration()
                .setLogSegmentRollingIntervalMinutes(60) // interval to roll log segment
                .setRetentionPeriodHours(1) // retention period
                .setWriteQuorumSize(2) // 2 replicas
                .setAckQuorumSize(2) // 2 replicas
                .setEnsembleSize(3); // how many hosts to store a log segment
        // Create a distributedlog
        DistributedLogNamespace namespace = DistributedLogNamespaceBuilder.newBuilder()
                .conf(conf)
                .uri(uri)
                .build();

        DistributedLogManager unpartitionedDLM =
            namespace.openLog("unpartitioned-example");
        System.out.println("Create unpartitioned stream : unpartitioned-example");
        LogWriter unpartitionedWriter = unpartitionedDLM.startLogSegmentNonPartitioned();
        for (long i = 1; i <= 10; i++) {
            LogRecord record = new LogRecord(i, generatePayload("unpartitioned-example", i));
            unpartitionedWriter.write(record);
        }
        unpartitionedWriter.close();
        System.out.println("Write 10 records into unpartitioned stream.");
        LogReader unpartitionedReader = unpartitionedDLM.getInputStream(1);
        System.out.println("Read unpartitioned stream : unpartitioned-example");
        LogRecord unpartitionedRecord = unpartitionedReader.readNext(false);
        while (null != unpartitionedRecord) {
            System.out.println(String.format("txn %d : %s",
                    unpartitionedRecord.getTransactionId(), new String(unpartitionedRecord.getPayload(), "UTF-8")));
            unpartitionedRecord = unpartitionedReader.readNext(false);
        }
        unpartitionedReader.close();
        System.out.println("Read unpartitioned stream done.");
        System.out.println("Read unpartitioned stream : unpartitioned-example from txn 5");
        LogReader unpartitionedReader2 = unpartitionedDLM.getInputStream(5);
        LogRecord unpartitionedRecord2 = unpartitionedReader2.readNext(false);
        while (null != unpartitionedRecord2) {
            System.out.println(String.format("txn %d : %s",
                    unpartitionedRecord2.getTransactionId(), new String(unpartitionedRecord2.getPayload(), "UTF-8")));
            unpartitionedRecord2 = unpartitionedReader2.readNext(false);
        }
        unpartitionedReader2.close();
        System.out.println("Read unpartitioned stream done.");
        unpartitionedDLM.delete();
        unpartitionedDLM.close();
    }
}
