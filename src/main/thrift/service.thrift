namespace java com.twitter.distributedlog.thrift.service

service DistributedLogService {
    string write(string stream, binary data)
}
