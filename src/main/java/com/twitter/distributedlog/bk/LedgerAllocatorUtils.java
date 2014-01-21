package com.twitter.distributedlog.bk;

import com.twitter.distributedlog.BookKeeperClient;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.ZooKeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static com.twitter.distributedlog.DistributedLogConstants.ZK_VERSION;
import static com.twitter.distributedlog.DistributedLogConstants.ZK33;

public class LedgerAllocatorUtils {

    static final Logger LOG = LoggerFactory.getLogger(LedgerAllocatorUtils.class);

    static Class<? extends LedgerAllocator> LEDGER_ALLOCATOR_POOL_CLASS = null;
    static final Class[] LEDGER_ALLOCATOR_POOL_ARGS = {
        String.class, int.class, DistributedLogConfiguration.class,
        ZooKeeperClient.class, BookKeeperClient.class
    };

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
            String poolPath, int corePoolSize,
            DistributedLogConfiguration conf,
            ZooKeeperClient zkc, BookKeeperClient bkc) throws IOException {
        if (ZK_VERSION.getVersion().equals(ZK33)) {
            return null;
        } else {
            if (null == LEDGER_ALLOCATOR_POOL_CLASS) {
                try {
                    LEDGER_ALLOCATOR_POOL_CLASS = (Class<? extends LedgerAllocator>)
                            Class.forName("com.twitter.distributedlog.bk.LedgerAllocatorPool", true,
                                    LedgerAllocator.class.getClassLoader());
                    LOG.info("Instantiate ledger allocator pool class : {}", LEDGER_ALLOCATOR_POOL_CLASS);
                } catch (ClassNotFoundException cnfe) {
                    throw new IOException("Can't initialize the ledger allocator pool : ", cnfe);
                }
            }
            // create new instance
            Constructor<? extends LedgerAllocator> constructor;
            try {
                constructor = LEDGER_ALLOCATOR_POOL_CLASS.getDeclaredConstructor(LEDGER_ALLOCATOR_POOL_ARGS);
            } catch (NoSuchMethodException nsme) {
                throw new IOException("No constructor found for ledger allocator pool class "
                        + LEDGER_ALLOCATOR_POOL_CLASS + " : ", nsme);
            }
            Object[] arguments = {
                    poolPath, corePoolSize, conf, zkc, bkc
            };
            try {
                return constructor.newInstance(arguments);
            } catch (InstantiationException e) {
                throw new IOException("Failed to instantiate ledger allocator pool : ", e);
            } catch (IllegalAccessException e) {
                throw new IOException("Encountered illegal access when instantiating ledger allocator pool : ", e);
            } catch (InvocationTargetException e) {
                throw new IOException("Encountered invocation target exception when instantiating ledger allocator pool : ", e);
            }
        }
    }
}
