namespace java com.twitter.distributedlog.thrift.messaging

struct TransformedRecord {
    1: required binary payload
    2: optional binary srcDlsn
}
