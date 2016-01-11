package com.twitter.distributedlog.feature;

import com.twitter.distributedlog.DistributedLogConfiguration;

/**
 * Utils for deciders
 */
public class DeciderUtils {

    //
    // Decider Feature Provider Settings
    //
    private static final String BKDL_DECIDER_BASE_CONFIG_PATH = "deciderBaseConfigPath";
    private static final String BKDL_DECIDER_BASE_CONFIG_PATH_DEFAULT = "decider.yml";
    private static final String BKDL_DECIDER_OVERLAY_CONFIG_PATH = "deciderOverlayConfigPath";
    private static final String BKDL_DECIDER_OVERLAY_CONFIG_PATH_DEFAULT = null;
    private static final String BKDL_DECIDER_ENVIRONMENT = "deciderEnvironment";
    private static final String BKDL_DECIDER_ENVIRONMENT_DEFAULT = null;

    /**
     * Get the base config path for decider.
     *
     * @param conf distributedlog configuration
     * @return base config path for decider.
     */
    public static String getDeciderBaseConfigPath(DistributedLogConfiguration conf) {
        return conf.getString(BKDL_DECIDER_BASE_CONFIG_PATH, BKDL_DECIDER_BASE_CONFIG_PATH_DEFAULT);
    }

    /**
     * Set the base config path for decider.
     *
     * @param conf
     *          distributedlog configuration
     * @param configPath
     *          base config path for decider.
     */
    public static void setDeciderBaseConfigPath(DistributedLogConfiguration conf,
                                                String configPath) {
        conf.setProperty(BKDL_DECIDER_BASE_CONFIG_PATH, configPath);
    }

    /**
     * Get the overlay config path for decider.
     *
     * @param conf distributedlog configuration
     * @return overlay config path for decider.
     */
    public static String getDeciderOverlayConfigPath(DistributedLogConfiguration conf) {
        return conf.getString(BKDL_DECIDER_OVERLAY_CONFIG_PATH, BKDL_DECIDER_OVERLAY_CONFIG_PATH_DEFAULT);
    }

    /**
     * Set the overlay config path for decider.
     *
     * @param conf distributedlog configuration
     * @param configPath
     *          overlay config path for decider.
     */
    public static void setDeciderOverlayConfigPath(DistributedLogConfiguration conf,
                                                   String configPath) {
        conf.setProperty(BKDL_DECIDER_OVERLAY_CONFIG_PATH, configPath);
    }

    /**
     * Get the decider environment.
     *
     * @param conf distributedlog configuration
     * @return decider environment
     */
    public String getDeciderEnvironment(DistributedLogConfiguration conf) {
        return conf.getString(BKDL_DECIDER_ENVIRONMENT, BKDL_DECIDER_ENVIRONMENT_DEFAULT);
    }

    /**
     * Set the decider environment.
     *
     * @param conf distributedlog configuration
     * @param environment decider environment
     * @return distributedlog configuration.
     */
    public void setDeciderEnvironment(DistributedLogConfiguration conf, String environment) {
        conf.setProperty(BKDL_DECIDER_ENVIRONMENT, environment);
    }

}
