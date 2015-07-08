package com.twitter.distributedlog.service.stream;

import com.twitter.util.Future;
import java.io.IOException;

public interface StreamManager {
    Future<Void> deleteAndRemoveAsync(String stream);
    Future<Void> closeAndRemoveAsync(String stream);
}
