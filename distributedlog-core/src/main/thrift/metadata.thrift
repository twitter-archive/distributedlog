namespace java com.twitter.distributedlog.thrift

struct BKDLConfigFormat {
    1: optional string bkZkServers
    2: optional string bkLedgersPath
    3: optional bool sanityCheckTxnID
    4: optional bool encodeRegionID
    5: optional string bkZkServersForReader
    6: optional string dlZkServersForWriter
    7: optional string dlZkServersForReader
    8: optional string aclRootPath
    9: optional i64 firstLedgerSeqNo
}

struct AccessControlEntry {
    1: optional bool denyWrite
    2: optional bool denyTruncate
    3: optional bool denyDelete
    4: optional bool denyAcquire
    5: optional bool denyRelease
}
