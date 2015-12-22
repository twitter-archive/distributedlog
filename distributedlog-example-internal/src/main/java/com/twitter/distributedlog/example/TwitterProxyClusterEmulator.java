package com.twitter.distributedlog.example;

import com.google.common.base.Optional;
import com.twitter.app.Flag;
import com.twitter.app.Flaggable;
import com.twitter.distributedlog.service.DistributedLogCluster;
import com.twitter.distributedlog.service.DistributedLogTwitterServer;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

/**
 * Twitter Server Based DistributedLog Write Proxy cluster emulator
 */
public class TwitterProxyClusterEmulator extends DistributedLogTwitterServer {

    static final Logger logger = LoggerFactory.getLogger(DistributedLogTwitterServer.class);

    private final Flag<String> bkConfFlag =
            flag().create("bkconf", "", "Path to bk configuration", Flaggable.ofString());

    @Override
    public void main() throws Throwable {
        DistributedLogCluster dlCluster = null;
        try {
            dlCluster = DistributedLogCluster.newBuilder()
                    .numBookies(3)
                    .shouldStartZK(true)
                    .zkServers("127.0.0.1")
                    .shouldStartProxy(false)
                    .bkConf(loadBkConf(bkConfFlag.apply()))
                    .build();
            dlCluster.start();
            runServer(Optional.of(dlCluster.getUri().toString()));
        } catch (Exception ex) {
            if (null != dlCluster) {
                dlCluster.stop();
            }
            System.out.println("Exception occurred running emulator " + ex);
        }
    }

    private static ServerConfiguration loadBkConf(String confFile) {
        ServerConfiguration conf = new ServerConfiguration();
        try {
            if (null != confFile) {
                URL confUrl = new File(confFile).toURI().toURL();
                conf.loadConf(confUrl);
                logger.info("Loaded bk conf at {}.", confUrl);
            }
        } catch (Exception ex) {
            logger.warn("Loading bk conf failed.", ex);
        }
        return conf;
    }

    /**
     * Wrapper to run DistributedLog Twitter Server
     */
    public static class Main {

        public static void main(String[] args) throws Exception {
            new TwitterProxyClusterEmulator().main(args);
        }

    }
}
