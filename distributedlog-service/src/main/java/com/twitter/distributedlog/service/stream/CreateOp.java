package com.twitter.distributedlog.service.stream;

import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.service.ResponseUtils;
import com.twitter.distributedlog.thrift.service.WriteResponse;
import com.twitter.distributedlog.util.Sequencer;
import com.twitter.util.Future;
import org.apache.bookkeeper.feature.Feature;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.runtime.AbstractFunction1;

public class CreateOp extends AbstractWriteOp {
  private final StreamManager streamManager;

  public CreateOp(String stream,
                  StatsLogger statsLogger,
                  StreamManager streamManager,
                  Long checksum,
                  Feature checksumEnabledFeature) {
    super(stream, requestStat(statsLogger, "create"), checksum, checksumEnabledFeature);
    this.streamManager = streamManager;
  }

  @Override
  protected Future<WriteResponse> executeOp(AsyncLogWriter writer,
                                            Sequencer sequencer,
                                            Object txnLock) {
    Future<Void> result = streamManager.createStreamAsync(streamName());
    return result.map(new AbstractFunction1<Void, WriteResponse>() {
      @Override
      public WriteResponse apply(Void value) {
        return ResponseUtils.writeSuccess();
      }
    });
  }
}
