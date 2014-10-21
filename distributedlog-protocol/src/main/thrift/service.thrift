namespace java com.twitter.distributedlog.thrift.service

// Response stats codes
enum StatusCode {
    // 2xx: action requested by the client was received, understood, accepted and processed successfully.

    // standard response for successful requests.
    SUCCESS = 200,

    // 3xx: client must take additional action to complete the request.
    // client closed.
    CLIENT_CLOSED = 301,
    // found the stream in a different server, a redirection is required by client.
    FOUND = 302,

    // 4xx: client seems to have erred.

    // request cannot be fulfilled due to bad syntax.
    BAD_REQUEST = 400,
    // request record too large
    TOO_LARGE_RECORD = 413,

    // 5xx: server failed to fulfill an apparently valid request.

    // Generic error message, given when no more specific message is suitable.
    INTERNAL_SERVER_ERROR = 500,
    // Not implemented
    NOT_IMPLEMENTED = 501,
    // Already Closed Exception
    ALREADY_CLOSED = 502,
    // Service is currently unavailable (because it is overloaded or down for maintenance).
    SERVICE_UNAVAILABLE = 503,
    // Locking exception
    LOCKING_EXCEPTION = 504,
    // ZooKeeper Errors
    ZOOKEEPER_ERROR = 505,
    // Metadata exception
    METADATA_EXCEPTION = 506,
    // BK Transmit Error
    BK_TRANSMIT_ERROR = 507,
    // Flush timeout
    FLUSH_TIMEOUT = 508,
    // Log empty
    LOG_EMPTY = 509,
    // Log not found
    LOG_NOT_FOUND = 510,
    // Truncated Transactions
    TRUNCATED_TRANSACTION = 511,
    // End of Stream
    END_OF_STREAM = 512,
    // Transaction Id Out of Order
    TRANSACTION_OUT_OF_ORDER = 513,
    // Write exception
    WRITE_EXCEPTION = 514,
    // Stream Unavailable
    STREAM_UNAVAILABLE = 515,
    // Write cancelled exception
    WRITE_CANCELLED_EXCEPTION = 516,
    // over-capacity/backpressure
    OVER_CAPACITY = 517,

    // 6xx: unexpected
    UNEXPECTED = 600,
    INTERRUPTED = 601,
    INVALID_STREAM_NAME = 602,
    ILLEGAL_STATE = 603,

    // 10xx: reader exceptions
    RETRYABLE_READ = 1000,
    LOG_READ_ERROR = 1001,
    // Read cancelled exception
    READ_CANCELLED_EXCEPTION = 1002,
}

// Response Header
struct ResponseHeader {
    1: required StatusCode code;
    2: optional string errMsg;
    3: optional string location;
}

// Write Response
struct WriteResponse {
    1: required ResponseHeader header;
    2: optional string dlsn;
}

// Bulk write response
struct BulkWriteResponse {
    1: required ResponseHeader header;
    2: optional list<WriteResponse> writeResponses;
}

// Write Context
struct WriteContext {
    1: optional set<string> triedHosts;
}

// Server Info
struct ServerInfo {
    1: optional map<string, string> ownerships;
}

service DistributedLogService {

    ServerInfo handshake();

    WriteResponse heartbeat(string stream, WriteContext ctx);

    WriteResponse write(string stream, binary data);

    WriteResponse writeWithContext(string stream, binary data, WriteContext ctx);

    BulkWriteResponse writeBulkWithContext(string stream, list<binary> data, WriteContext ctx);

    WriteResponse truncate(string stream, string dlsn, WriteContext ctx);

    WriteResponse release(string stream, WriteContext ctx);

    WriteResponse delete(string stream, WriteContext ctx);
}
