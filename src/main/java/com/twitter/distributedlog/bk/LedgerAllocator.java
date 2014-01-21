package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.util.Allocator;
import org.apache.bookkeeper.client.LedgerHandle;

public interface LedgerAllocator extends Allocator<LedgerHandle, Object, Object> {
}
