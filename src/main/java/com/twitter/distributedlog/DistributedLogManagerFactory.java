package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogManagerFactory {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogManagerFactory.class);

    public static DistributedLogManager createDistributedLogManager(String name, URI uri) throws IOException, IllegalArgumentException {
        return createDistributedLogManager(name, new DistributedLogConfiguration(), uri);
    }

    private static void validateInput(DistributedLogConfiguration conf, URI uri) throws IllegalArgumentException {
        // input validation
        //
        if (null == conf) {
            throw new IllegalArgumentException("Incorrect Configuration");
        }

        if ((null == uri) || (null == uri.getAuthority()) || (null == uri.getPath())) {
            throw new IllegalArgumentException("Incorrect ZK URI");
        }
    }


    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri);
        return new BKDistributedLogManager(name, conf, uri);
    }

    public static boolean checkIfLogExists(DistributedLogConfiguration conf, URI uri, String name)
        throws IOException, InterruptedException, IllegalArgumentException {
        validateInput(conf, uri);
        String logRootPath = conf.getDLZKPathPrefix() + uri.getPath() + String.format("/%s", name);
        ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
        try {
            if (null != zkc.get().exists(logRootPath, false)) {
                return true;
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted checkIfLogExists  " + logRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + logRootPath + "entry in zookeeper", ke);
        }

        return false;
    }

    public static Collection<String> enumerateAllLogsInNamespace(DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        validateInput(conf, uri);
        String namespaceRootPath = conf.getDLZKPathPrefix() + uri.getPath();
        ZooKeeperClient zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
        try {
            return zkc.get().getChildren(namespaceRootPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new IOException("Interrupted while deleting " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
            throw new IOException("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
        }
    }
}
