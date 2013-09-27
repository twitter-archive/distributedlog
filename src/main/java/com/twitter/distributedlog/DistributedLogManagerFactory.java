package com.twitter.distributedlog;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
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
        return DistributedLogManagerFactory.enumerateAllLogsInternal(zooKeeperClient, conf, namespace);
    }

    public Map<String, byte[]> enumerateLogsWithMetadataInNamespace()
        throws IOException, InterruptedException, IllegalArgumentException {
        return DistributedLogManagerFactory.enumerateLogsWithMetadataInternal(zooKeeperClient, conf, namespace);
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
        } finally {
            zkc.close();
        }

        return false;
    }

    public static Collection<String> enumerateAllLogsInNamespace(DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        return enumerateAllLogsInternal(null, conf, uri);
    }

    private static Collection<String> enumerateAllLogsInternal(ZooKeeperClient zkcShared, DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        validateInput(conf, uri);
        String namespaceRootPath = conf.getDLZKPathPrefix() + uri.getPath();
        ZooKeeperClient zkc = zkcShared;
        if (null == zkcShared) {
            zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
        }
        try {
            Stat currentStat = zkc.get().exists(namespaceRootPath, false);
            if (currentStat == null) {
                return new LinkedList<String>();
            }
            return zkc.get().getChildren(namespaceRootPath, false);
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new IOException("Interrupted while deleting " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
            throw new IOException("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
        } finally {
            if (null == zkcShared) {
                zkc.close();
            }
        }
    }

    public static Map<String, byte[]> enumerateLogsWithMetadataInNamespace(DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        return enumerateLogsWithMetadataInternal(null, conf, uri);
    }


    private static Map<String, byte[]> enumerateLogsWithMetadataInternal(ZooKeeperClient zkcShared, DistributedLogConfiguration conf, URI uri)
        throws IOException, InterruptedException, IllegalArgumentException {
        validateInput(conf, uri);
        String namespaceRootPath = conf.getDLZKPathPrefix() + uri.getPath();
        ZooKeeperClient zkc = zkcShared;
        if (null == zkcShared) {
            zkc = new ZooKeeperClient(conf.getZKSessionTimeoutSeconds(), uri);
        }
        HashMap<String, byte[]> result = new HashMap<String, byte[]>();

        try {
            Stat currentStat = zkc.get().exists(namespaceRootPath, false);
            if (currentStat == null) {
                return result;
            }
            List<String> children = zkc.get().getChildren(namespaceRootPath, false);
            for(String child: children) {
                String zkPath = String.format("%s/%s", namespaceRootPath, child);
                currentStat = zkc.get().exists(zkPath, false);
                if (currentStat == null) {
                    result.put(child, new byte[0]);
                } else {
                    result.put(child, zkc.get().getData(zkPath, false, currentStat));
                }
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted while deleting " + namespaceRootPath, ie);
            throw new IOException("Interrupted while reading " + namespaceRootPath, ie);
        } catch (KeeperException ke) {
            LOG.error("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
            throw new IOException("Error reading" + namespaceRootPath + "entry in zookeeper", ke);
        } finally {
            if (null == zkcShared) {
                zkc.close();
            }
        }
        return result;
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
