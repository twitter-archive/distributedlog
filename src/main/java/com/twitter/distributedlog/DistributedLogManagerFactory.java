package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedLogManagerFactory {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogManagerFactory.class);

    private DistributedLogConfiguration conf;
    private URI namespace;
    private ZooKeeperClient zooKeeperClient = null;
    private BookKeeperClient bookKeeperClient = null;

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri) throws IOException, IllegalArgumentException {
        validateInput(conf, uri);
        this.conf = conf;
        this.namespace = uri;

        try {
            if (!this.conf.getSeparateZKClients()) {
                zooKeeperClient = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
            }

            if (!this.conf.getSeparateBKClients()) {
                if (conf.getShareZKClientWithBKC()) {
                    this.bookKeeperClient = new BookKeeperClient(conf, zooKeeperClient, String.format("%s:shared", namespace));
                } else {
                    this.bookKeeperClient = new BookKeeperClient(conf, uri.getAuthority().replace(";", ","), String.format("%s:shared", namespace));
                }
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while accessing ZK", ie);
            throw new IOException("Error initializing zk", ie);
        } catch (KeeperException ke) {
            LOG.error("Error accessing entry in zookeeper", ke);
            throw new IOException("Error initializing zk", ke);
        }
    }

    public DistributedLogManager createDistributedLogManager(String nameOfLogStream) throws IOException, IllegalArgumentException {
        return DistributedLogManagerFactory.createDistributedLogManager(nameOfLogStream, this.conf, namespace, zooKeeperClient, bookKeeperClient);
    }

    public boolean checkIfLogExists(String nameOfLogStream)
        throws IOException, InterruptedException, IllegalArgumentException {
        return DistributedLogManagerFactory.checkIfLogExists(conf, namespace, nameOfLogStream);
    }

    public Collection<String> enumerateAllLogsInNamespace()
        throws IOException, InterruptedException, IllegalArgumentException {
        return DistributedLogManagerFactory.enumerateAllLogsInNamespace(conf, namespace);
    }

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
        return DistributedLogManagerFactory.createDistributedLogManager(name, conf, uri, null, null);
    }

    public static DistributedLogManager createDistributedLogManager(String name, DistributedLogConfiguration conf, URI uri, ZooKeeperClient zkc, BookKeeperClient bkc)
        throws IOException, IllegalArgumentException {
        validateInput(conf, uri);
        return new BKDistributedLogManager(name, conf, uri, zkc, bkc);
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

    /**
     * Close the distributed log manager factory, freeing any resources it may hold.
     */
    public void close() throws IOException {
        try {
            if (null != bookKeeperClient) {
                bookKeeperClient.release();
            }
            if (null != zooKeeperClient) {
                zooKeeperClient.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception while closing distributed log manager factory", e);
        }
    }
}
