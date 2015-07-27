package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

public class LedgerAllocatorUtils {

    /**
     * Create ledger allocator pool.
     *
     * @param poolPath
     *          ledger allocator pool path.
     * @param corePoolSize
     *          ledger allocator pool core size.
     * @param conf
     *          distributedlog configuration.
     * @param zkc
     *          zookeeper client
     * @param bkc
     *          bookkeeper client
     * @return ledger allocator
     * @throws IOException
     */
    public static LedgerAllocator createLedgerAllocatorPool(
            String poolPath,
            int corePoolSize,
            DistributedLogConfiguration conf,
            ZooKeeperClient zkc,
            BookKeeperClient bkc,
            ScheduledExecutorService scheduledExecutorService) throws IOException {
        return new LedgerAllocatorPool(poolPath, corePoolSize, conf, zkc, bkc, scheduledExecutorService);
    }
}
