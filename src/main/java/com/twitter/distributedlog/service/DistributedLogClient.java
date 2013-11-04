package com.twitter.distributedlog.service;

import com.twitter.distributedlog.thrift.service.DistributedLogService;

public interface DistributedLogClient extends DistributedLogService.ServiceIface {
    void close();
}
