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
struct UpdateReadPositionsRequest {
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
// Messages
//

// Coordinator -> Worker: Command Request
struct ReaderCommandRequest {
    1: required i32 type
    2: optional StartReadRequest start_read_request
    3: optional StopReadRequest stop_read_request
}

// Worker -> Coordinator: Command Response
struct ReaderCommandResponse {
    1: required i32 type
    2: optional StartReadResponse start_read_response
    3: optional StopReadResponse stop_read_response
}
