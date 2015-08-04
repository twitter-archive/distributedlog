package com.twitter.distributedlog;

import com.google.common.base.Optional;
import com.twitter.distributedlog.acl.AccessControlManager;
import com.twitter.distributedlog.callback.NamespaceListener;
import com.twitter.distributedlog.config.DynamicDistributedLogConfiguration;
import com.twitter.distributedlog.exceptions.InvalidStreamNameException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;

/**
 * This is the legacy way to access bookkeeper based distributedlog namespace.
 * Use {@link DistributedLogNamespace} to manage logs instead if you could.
 */
@Deprecated
public class DistributedLogManagerFactory {
    static final Logger LOG = LoggerFactory.getLogger(DistributedLogManagerFactory.class);

    public static enum ClientSharingOption {
        PerStreamClients,
        SharedZKClientPerStreamBKClient,
        SharedClients
    }

    private final BKDistributedLogNamespace namespace;

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri)
            throws IOException, IllegalArgumentException {
        this(conf, uri, NullStatsLogger.INSTANCE);
    }

    public DistributedLogManagerFactory(DistributedLogConfiguration conf, URI uri,
                                        StatsLogger statsLogger)
            throws IOException, IllegalArgumentException {
        this(conf,
             uri,
             statsLogger,
             DistributedLogConstants.UNKNOWN_CLIENT_ID,
             DistributedLogConstants.LOCAL_REGION_ID);
    }

    public DistributedLogManagerFactory(DistributedLogConfiguration conf,
                                        URI uri,
                                        StatsLogger statsLogger,
                                        String clientId,
                                        int regionId)
            throws IOException, IllegalArgumentException {
        this.namespace = BKDistributedLogNamespace.newBuilder()
                .conf(conf)
                .uri(uri)
                .statsLogger(statsLogger)
                .clientId(clientId)
                .regionId(regionId)
                .build();
    }

    public DistributedLogNamespace getNamespace() {
        return namespace;
    }

    public void registerNamespaceListener(NamespaceListener listener) {
        namespace.registerNamespaceListener(listener);
    }

    /**
     * Create a DistributedLogManager for <i>nameOfLogStream</i>, with default shared clients.
     *
     * @param nameOfLogStream
     *          name of log stream
     * @return distributedlog manager
     * @throws com.twitter.distributedlog.exceptions.InvalidStreamNameException if stream name is invalid
     * @throws IOException
     */
    public DistributedLogManager createDistributedLogManagerWithSharedClients(String nameOfLogStream)
        throws InvalidStreamNameException, IOException {
        return createDistributedLogManager(nameOfLogStream, ClientSharingOption.SharedClients);
    }

    /**
     * Create a DistributedLogManager for <i>nameOfLogStream</i>, with specified client sharing options.
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @param clientSharingOption
     *          specifies if the ZK/BK clients are shared
     * @return distributedlog manager instance.
     * @throws com.twitter.distributedlog.exceptions.InvalidStreamNameException if stream name is invalid
     * @throws IOException
     */
    public DistributedLogManager createDistributedLogManager(
            String nameOfLogStream,
            ClientSharingOption clientSharingOption)
        throws InvalidStreamNameException, IOException {
        Optional<DistributedLogConfiguration> streamConfiguration = Optional.absent();
        Optional<DynamicDistributedLogConfiguration> dynamicStreamConfiguration = Optional.absent();
        return createDistributedLogManager(nameOfLogStream,
            clientSharingOption,
            streamConfiguration,
            dynamicStreamConfiguration);
    }

    /**
     * Create a DistributedLogManager for <i>nameOfLogStream</i>, with specified client sharing options.
     * This method allows the caller to override global configuration options by supplying stream
     * configuration overrides. Stream config overrides come in two flavors, static and dynamic. Static
     * config never changes, and DynamicDistributedLogConfiguration is a) reloaded periodically and
     * b) safe to access from any context.
     *
     * @param nameOfLogStream
     *          name of log stream.
     * @param clientSharingOption
     *          specifies if the ZK/BK clients are shared
     * @param streamConfiguration
     *          stream configuration overrides.
     * @param dynamicStreamConfiguration
     *          dynamic stream configuration overrides.
     * @return distributedlog manager instance.
     * @throws com.twitter.distributedlog.exceptions.InvalidStreamNameException if stream name is invalid
     * @throws IOException
     */
    public DistributedLogManager createDistributedLogManager(
            String nameOfLogStream,
            ClientSharingOption clientSharingOption,
            Optional<DistributedLogConfiguration> streamConfiguration,
            Optional<DynamicDistributedLogConfiguration> dynamicStreamConfiguration)
        throws InvalidStreamNameException, IOException {
        return namespace.createDistributedLogManager(
            nameOfLogStream,
            clientSharingOption,
            streamConfiguration,
            dynamicStreamConfiguration);
    }

    public MetadataAccessor createMetadataAccessor(String nameOfMetadataNode)
            throws InvalidStreamNameException, IOException {
        return namespace.createMetadataAccessor(nameOfMetadataNode);
    }

    public synchronized AccessControlManager createAccessControlManager() throws IOException {
        return namespace.createAccessControlManager();
    }

    public boolean checkIfLogExists(String nameOfLogStream)
        throws IOException, IllegalArgumentException {
        return namespace.logExists(nameOfLogStream);
    }

    public Collection<String> enumerateAllLogsInNamespace()
        throws IOException, IllegalArgumentException {
        return namespace.enumerateAllLogsInNamespace();
    }

    public Map<String, byte[]> enumerateLogsWithMetadataInNamespace()
        throws IOException, IllegalArgumentException {
        return namespace.enumerateLogsWithMetadataInNamespace();
    }

    /**
     * This method is to initialize the metadata for a unpartitioned stream with name <i>streamName</i>.
     *
     * TODO: after 0.2 is upgraded to 0.3, remove this.
     *
     * @param streamName
     *          stream name.
     * @throws IOException
     */
    public void createUnpartitionedStream(final String streamName) throws IOException {
        namespace.createLog(streamName);
    }

    /**
     * Close the distributed log manager factory, freeing any resources it may hold.
     */
    public void close() {
        namespace.close();
    }
}
