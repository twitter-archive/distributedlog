namespace java com.twitter.distributedlog.v2.benchmark.thrift

struct Message {
    1: i64 publishTime;
    2: binary payLoad;
}
