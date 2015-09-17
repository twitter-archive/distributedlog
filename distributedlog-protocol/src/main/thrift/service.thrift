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

    // request is denied for some reason
    REQUEST_DENIED = 403,
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
    // stream exists but is not ready (recovering etc.).
    // the difference between NOT_READY and UNAVAILABLE is that UNAVAILABLE
    // indicates the stream is no longer owned by the proxy and we should
    // redirect. NOT_READY indicates the stream exist at the proxy but isn't
    // ready for writes.
    STREAM_NOT_READY = 518,
    // Region Unavailable
    REGION_UNAVAILABLE = 519,
    // Invalid Enveloped Entry
    INVALID_ENVELOPED_ENTRY = 520,
    // Unsupported metadata version
    UNSUPPORTED_METADATA_VERSION = 521,
    // Log Already Exists
    LOG_EXISTS = 522,

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

// HeartBeat Options
struct HeartbeatOptions {
    1: optional bool sendHeartBeatToReader;
}

// Server Status
enum ServerStatus {
    // service is writing and accepting new streams
    WRITE_AND_ACCEPT    = 100,
    // service is only writing to old streams, not accepting new streams
    WRITE_ONLY          = 200,
    // service is shutting down, will not write
    DOWN                = 300,
}

// Server Info
struct ServerInfo {
    1: optional map<string, string> ownerships;
    2: optional ServerStatus serverStatus;
}

// Client Info
struct ClientInfo {
    1: optional string streamNameRegex;
    2: optional bool getOwnerships;
}

service DistributedLogService {

    ServerInfo handshake();

    ServerInfo handshakeWithClientInfo(ClientInfo clientInfo);

    WriteResponse heartbeat(string stream, WriteContext ctx);

    WriteResponse heartbeatWithOptions(string stream, WriteContext ctx, HeartbeatOptions options);

    WriteResponse write(string stream, binary data);

    WriteResponse writeWithContext(string stream, binary data, WriteContext ctx);

    BulkWriteResponse writeBulkWithContext(string stream, list<binary> data, WriteContext ctx);

    WriteResponse truncate(string stream, string dlsn, WriteContext ctx);

    WriteResponse release(string stream, WriteContext ctx);

    WriteResponse delete(string stream, WriteContext ctx);

    // Admin Methods
    void setAcceptNewStream(bool enabled);
}
