package com.twitter.distributedlog.service.stream;

import com.twitter.util.Future;

public interface StreamManager {
    Future<Void> deleteAndRemoveAsync(String stream);
    Future<Void> closeAndRemoveAsync(String stream);
}
