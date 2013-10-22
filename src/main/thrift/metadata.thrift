namespace java com.twitter.distributedlog.thrift

struct BKDLConfigFormat {
    1: optional string bkZkServers
    2: optional string bkLedgersPath
}
