package com.twitter.distributedlog.group;

public class Constants {

    // Coordinator -> Coordinator
    public static final int COORDINATOR_BOOTSTRAP_REQ   = 0;
    public static final int COORDINATOR_SNAPSHOT_REQ    = 0;

    // Coordinator <-> Worker
    public static final int JOIN_GROUP_REQ      = 101;
    public static final int JOIN_GROUP_RESP     = 102;
    public static final int LEAVE_GROUP_REQ     = 103;
    public static final int LEAVE_GROUP_RESP    = 104;
    public static final int RENEW_LEASE_REQ     = 105;
    public static final int RENEW_LEASE_RESP    = 106;
    public static final int COMMAND_REQ         = 107;
    public static final int COMMAND_RESP        = 108;

    // Status Code

    // General Status Code
    public static final int OK                  = 0;
    public static final int ERROR               = -1;

    // Specific Status Code
    public static final int LEASE_EXPIRED       = -101;

}
