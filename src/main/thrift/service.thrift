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
    // Service is currently unavailable (because it is overloaded or down for maintenance).
    SERVICE_UNAVAILABLE = 503,
    // Locking exception
    LOCKING_EXCEPTION = 504,
    // ZooKeeper Errors
    ZOOKEEPER_ERROR = 505,
    // Metadata exception
    METADATA_EXCEPTION = 506,

    // 6xx: unexpected
    UNEXPECTED = 600,
    INTERRUPTED = 601
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

// Write Context
struct WriteContext {
    1: optional set<string> triedHosts;
}

// Server Info
struct ServerInfo {
}

service DistributedLogService {

    ServerInfo handshake();

    WriteResponse write(string stream, binary data);

    WriteResponse writeWithContext(string stream, binary data, WriteContext ctx);

    WriteResponse truncate(string stream, string dlsn, WriteContext ctx);

    WriteResponse release(string stream, WriteContext ctx);

    WriteResponse delete(string stream, WriteContext ctx);
}
