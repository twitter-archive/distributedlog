namespace java com.twitter.distributedlog.thrift.readergroup

//
// Reader Group on Group Protocol
//
// - A set of machines coordinate on reading a set of streams
//

struct ReadPosition {
    1: required binary dlsn
    2: required i64 sequence_id
}

// Worker -> Coordinator: Renew Lease
struct ReadPositions {
    1: required map<string, ReadPosition> positions
}

// Coordinator -> Worker: Start reading
struct StartReadRequest {
    1: required map<string, ReadPosition> streams
}

// Worker -> Coordinator: Start reading response
struct StartReadResponse {
    1: required i32 code
}

// Coordinator -> Worker: Stop reading
struct StopReadRequest {
    1: required list<string> streams
}

// Worker -> Coordinator: Stop reading response
struct StopReadResponse {
    1: required i32 code
}

//
