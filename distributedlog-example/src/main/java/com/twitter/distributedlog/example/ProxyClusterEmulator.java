package com.twitter.distributedlog.example;

import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.service.DistributedLogCluster;
import com.twitter.distributedlog.service.DistributedLogServerApp;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class for DistributedLogCluster emulator
 */
public class ProxyClusterEmulator {
    static final Logger LOG = LoggerFactory.getLogger(ProxyClusterEmulator.class);

    private final DistributedLogCluster dlCluster;
    private final String[] args;

    public ProxyClusterEmulator(String[] args) throws Exception {
        this.dlCluster = DistributedLogCluster.newBuilder()
            .numBookies(3)
            .shouldStartZK(true)
            .zkServers("127.0.0.1")
            .shouldStartProxy(false) // We'll start it separately so we can pass args.
            .dlConf(new DistributedLogConfiguration())
            .build();
        this.args = args;
    }

    public void start() throws Exception {
        dlCluster.start();

        // Run the server with bl cluster info.
        String[] extendedArgs = new String[args.length + 2];
        System.arraycopy(args, 0, extendedArgs, 0, args.length);
        extendedArgs[extendedArgs.length - 2] = "-u";
        extendedArgs[extendedArgs.length - 1] = dlCluster.getUri().toString();
        LOG.debug("Using args {}", Arrays.toString(extendedArgs));
        DistributedLogServerApp.main(extendedArgs);
    }

    public void stop() throws Exception {
        dlCluster.stop();
    }

    public static void main(String[] args) throws Exception {
        ProxyClusterEmulator emulator = null;
        try {
            emulator = new ProxyClusterEmulator(args);
            emulator.start();
        } catch (Exception ex) {
            if (null != emulator) {
                emulator.stop();
            }
            System.out.println("Exception occurred running emulator " + ex);
        }
    }
}
