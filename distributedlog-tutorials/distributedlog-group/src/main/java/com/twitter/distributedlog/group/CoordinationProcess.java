package com.twitter.distributedlog.group;

import com.twitter.distributedlog.thrift.group.CommandRequest;
import com.twitter.distributedlog.thrift.group.CommandResponse;
import com.twitter.distributedlog.thrift.group.JoinGroupRequest;
import com.twitter.distributedlog.thrift.group.JoinGroupResponse;
import com.twitter.distributedlog.thrift.group.LeaveGroupRequest;
import com.twitter.distributedlog.thrift.group.LeaveGroupResponse;
import com.twitter.distributedlog.thrift.group.RenewLeaseRequest;
import com.twitter.distributedlog.thrift.group.RenewLeaseResponse;

import java.util.List;

/**
 * Process to execute the coordination logic.
 */
public interface CoordinationProcess {

    /**
     * Callback when it becomes the coordinator
     */
    void becomeCoordinator(ControlChannelWriter writer);

    /**
     * Process member join <i>request</i>.
     *
     * @param request member join request.
     * @return result represents the join response.
     */
    JoinGroupResponse processMemberJoinRequest(JoinGroupRequest request);

    /**
     * Process member leave <i>request</i>.
     *
     * @param request member leave request.
     * @return result represents the leave response.
     */
    LeaveGroupResponse processMemberLeaveResponse(LeaveGroupRequest request);

    /**
     * Process renew lease request.
     *
     * @param request request to renew lease.
     * @return result represents the renew response.
     */
    RenewLeaseResponse renewLease(RenewLeaseRequest request);

    /**
     * Process command response.
     *
     * @param response command response from members.
     */
    void processCommandResponse(CommandResponse response);

    /**
     * Rebalance the membership topology to generate a set of command requests.
     *
     * @return a set of command requests after rebalanced.
     */
    List<CommandRequest> rebalance();

}
