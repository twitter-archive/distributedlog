package com.twitter.distributedlog.group;

import com.twitter.distributedlog.AsyncLogReader;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordSet;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.WriteException;
import com.twitter.distributedlog.group.dl.DLControlChannelWriter;
import com.twitter.distributedlog.group.dl.DLMembershipChannelReader;
import com.twitter.distributedlog.group.util.ThriftUtil;
import com.twitter.distributedlog.io.CompressionCodec;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.thrift.group.ControlMessage;
import com.twitter.distributedlog.thrift.group.MembershipMessage;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.distributedlog.util.Utils;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.twitter.distributedlog.group.Constants.*;

/**
 * Coordinator
 */
public class Coordinator implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(Coordinator.class);
    private final static int LOGRECORDSET_SIZE = 128 * 1024;

    private final String identity;
    private final CoordinationProcess coordinationProcess;
    private final DistributedLogNamespace namespace;
    private final String controlChannelStreamName;
    private final String membershipChannelStreamName;
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    public Coordinator(String identity,
                       CoordinationProcess coordinationProcess,
                       DistributedLogNamespace namespace,
                       String controlChannelStreamName,
                       String membershipChannelStreamName) {
        this.identity = identity;
        this.coordinationProcess = coordinationProcess;
        this.namespace = namespace;
        this.controlChannelStreamName = controlChannelStreamName;
        this.membershipChannelStreamName = membershipChannelStreamName;
    }

    @Override
    public void run() {
        DistributedLogManager controlChannelDLM = null;
        DistributedLogManager membershipChannelDLM = null;
        try {
            controlChannelDLM = namespace.openLog(controlChannelStreamName);
            AsyncLogWriter controlChannelLogWriter =
                    FutureUtils.result(controlChannelDLM.openAsyncLogWriter());
            ControlChannelWriter controlChannelWriter =
                    new DLControlChannelWriter(controlChannelLogWriter);
            // Coordination Process
            coordinationProcess.becomeCoordinator(controlChannelWriter);

            // Initiate the membership channel reader
            membershipChannelDLM = namespace.openLog(membershipChannelStreamName);
            AsyncLogReader membershipChannelLogReader =
                    FutureUtils.result(membershipChannelDLM.openAsyncLogReader(membershipChannelDLM.getLastDLSN()));
            MembershipChannelReader membershipChannelReader =
                    new DLMembershipChannelReader(membershipChannelLogReader);

            // process the membership stream
            processStream(membershipChannelReader, controlChannelWriter);

            stopLatch.await();
        } catch (Exception ioe) {
            logger.error("Encountered exception on running coordinator", ioe);
        } finally {
            Utils.closeQuietly(controlChannelDLM);
            Utils.closeQuietly(membershipChannelDLM);
        }

    }

    private void processStream(final MembershipChannelReader membershipChannelReader,
                               final ControlChannelWriter controlChannelWriter) {
        final FutureEventListener<List<MembershipMessage>> listener =
                new FutureEventListener<List<MembershipMessage>>() {
                    @Override
                    public void onFailure(Throwable cause) {
                        stopLatch.countDown();
                    }

                    @Override
                    public void onSuccess(List<MembershipMessage> messages) {
                        processMessages(messages, controlChannelWriter);
                        processStream(membershipChannelReader,
                                      controlChannelWriter);
                    }
                };
        membershipChannelReader.readMessages().addEventListener(listener);
    }

    private void processMessages(List<MembershipMessage> messages,
                                 ControlChannelWriter controlChannelWriter) {
        // send the responses in a transaction
        LogRecordSet.Writer recordSetWriter = LogRecordSet.newWriter(
                LOGRECORDSET_SIZE, CompressionCodec.Type.NONE);
        for (MembershipMessage msg : messages) {
            ControlMessage ctlMsg = new ControlMessage(COMMAND_REQ);
            switch (msg.getType()) {
                case JOIN_GROUP_REQ:
                    ctlMsg.setType(JOIN_GROUP_RESP);
                    ctlMsg.setJoin_group_response(
                            coordinationProcess.processMemberJoinRequest(msg.getJoin_group_request()));
                    break;
                case LEAVE_GROUP_REQ:
                    ctlMsg.setType(LEAVE_GROUP_RESP);
                    ctlMsg.setLeave_group_response(
                            coordinationProcess.processMemberLeaveResponse(msg.getLeave_group_request());
                    break;
                case RENEW_LEASE_REQ:
                    ctlMsg.setType(RENEW_LEASE_RESP);
                    ctlMsg.setRenew_lease_response(
                            coordinationProcess.renewLease(msg.getRenew_lease_request());
                    break;
                case COMMAND_RESP:
                    coordinationProcess.processCommandResponse(msg.getCommand_response());
                    ctlMsg = null;
                    break;
                default:
                    ctlMsg = null;
                    break;
            }
            if (null != ctlMsg) {
                LogRecord record;
                try {
                    record = ThriftUtil.write(0L, ctlMsg);
                } catch (TException e) {
                    stop();
                    return;
                }
                try {
                    recordSetWriter.writeRecord(
                            ByteBuffer.wrap(record.getPayload()),
                            new Promise<DLSN>());
                } catch (Exception e) {
                }
            }

        }
    }

    public void stop() {
        stopLatch.countDown();
    }
}
