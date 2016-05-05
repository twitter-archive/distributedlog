namespace java com.twitter.distributedlog.thrift.group

//
// The Group Protocol
//
// Roles: Coordinator & Worker
//
// - Worker joins a group.
// - Coordinator monitors the group and dispatch work items to workers
// - Worker receives commands from coordinator to execute certain actions: e.g. reading from a DL stream
// - Worker could work on its assigned item during the lease duration offered by Coordinator
// - Worker has to renew its lease before lease expiration
// - Coordinator tracks the leases of workers and also their progresses to balance the work items
// - Coordinator takes snapshot of assignment mapping for fast recovery when Coordinator fails
//
// Channels: Membership Channel & Control Channel
//
// - Membership Channel: Worker -> Coordinator
// - Control Channel: Coordinator -> Worker
// 

// Worker -> Coordinator
struct MembershipMessage {
    1: required i32 type;

}



// Worker -> Coordinator
struct JoinGroupRequest {
    1: required string identity
}

// Coordinator -> Worker
struct JoinGroupResponse {
    1: required string identity
    2: required i32 code
}

// Worker -> Coordinator
struct LeaveGroupRequest {
    1: required string identity
}

// Coordinator -> Worker
struct LeaveGroupResponse {
    1: required string identity
    2: required i32 code
}

// Worker -> Coordinator
struct RenewLeaseRequest {
    1: required string identity
    2: optional binary lease
}

// Coordinator -> Worker
struct RenewLeaseResponse {
    1: required string identity
    2: required i32 code
    2: optional i64 lease_duration
}

// Coordinator -> Worker
struct CommandRequest {
    1: required string identity
    2. optional binary command
}

// Worker -> Coordinator
struct CommandResponse {
    1: required string identity
    2. optional binary response
}

// Coordinator

struct SnapshotRequest {
    1: required i32 type;
    2: optional binary snapshot
}
