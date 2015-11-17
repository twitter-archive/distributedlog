namespace java com.twitter.distributedlog.v2.thrift

struct BKDLConfigFormat {
    1: optional string bkZkServers
    2: optional string bkLedgersPath
    3: optional bool sanityCheckTxnID
    4: optional bool encodeRegionID
    5: optional string bkZkServersForReader
    6: optional string dlZkServersForWriter
    7: optional string dlZkServersForReader
}
