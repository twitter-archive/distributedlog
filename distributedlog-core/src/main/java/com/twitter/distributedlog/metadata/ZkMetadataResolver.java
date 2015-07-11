package com.twitter.distributedlog.metadata;

import com.twitter.distributedlog.ZooKeeperClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.URI;

public class ZkMetadataResolver implements MetadataResolver {

    private final ZooKeeperClient zkc;

    public ZkMetadataResolver(ZooKeeperClient zkc) {
        this.zkc = zkc;
    }

    @Override
    public DLMetadata resolve(URI uri) throws IOException {
        String dlPath = uri.getPath();
        PathUtils.validatePath(dlPath);
        // Normal case the dl metadata is stored in the last segment
        // so lookup last segment first.
        String[] parts = StringUtils.split(dlPath, '/');
        if (null == parts || 0 == parts.length) {
            throw new IOException("Invalid dlPath to resolve dl metadata : " + dlPath);
        }
        for (int i = parts.length; i >= 0; i--) {
            String pathToResolve = String.format("/%s", StringUtils.join(parts, '/', 0, i));
            byte[] data;
            try {
                data = zkc.get().getData(pathToResolve, false, new Stat());
            } catch (KeeperException.NoNodeException nne) {
                continue;
            } catch (KeeperException ke) {
                throw new IOException("Fail to resolve dl path : " + pathToResolve);
            } catch (InterruptedException ie) {
                throw new IOException("Interrupted when resolving dl path : " + pathToResolve);
            }
            if (null == data || data.length == 0) {
                continue;
            }
            try {
                return DLMetadata.deserialize(uri, data);
            } catch (IOException ie) {
            }
        }
        throw new IOException("No bkdl config bound under dl path : " + dlPath);
    }
}
