package org.apache.bookkeeper.stats;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;

/**
 * Health Check Servlet Listener
 */
public class HealthCheckServletContextListener extends HealthCheckServlet.ContextListener {

    private final HealthCheckRegistry healthCheckRegistry;

    public HealthCheckServletContextListener(HealthCheckRegistry healthCheckRegistry) {
        this.healthCheckRegistry = healthCheckRegistry;
    }

    @Override
    protected HealthCheckRegistry getHealthCheckRegistry() {
        return healthCheckRegistry;
    }
}
