/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 */
package com.alibaba.druid.spring.boot.autoconfigure.stat;

import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fallback for non-Logback applications. Automatic output is disabled to avoid root-log pollution. */
final class DruidSlf4jMetricsEventSink implements DruidMetricsEventSink {
    private static final Logger LOG = LoggerFactory.getLogger(DruidSlf4jMetricsEventSink.class);
    private static final Logger EVENT_LOG = LoggerFactory.getLogger("druid.metrics.event");

    private volatile boolean applicationManaged;
    private volatile boolean warned;

    DruidSlf4jMetricsEventSink(DruidStatProperties.Prometheus config) {
        applicationManaged = !config.getLogging().isAutoConfigure();
        warnIfNecessary(config);
    }

    @Override
    public void log(String json, boolean abnormal) {
        if (!applicationManaged) return;
        if (abnormal) EVENT_LOG.warn(json); else EVENT_LOG.info(json);
    }

    @Override
    public boolean isActive() {
        return applicationManaged;
    }

    @Override
    public boolean refresh(DruidStatProperties.Prometheus previous, DruidStatProperties.Prometheus current) {
        applicationManaged = !current.getLogging().isAutoConfigure();
        warnIfNecessary(current);
        return true;
    }

    private void warnIfNecessary(DruidStatProperties.Prometheus config) {
        if (config.getLogging().isAutoConfigure() && !warned) {
            warned = true;
            LOG.warn("Druid metric event auto-configuration requires Logback; event logging is disabled. "
                    + "Set logging.auto-configure=false and configure logger druid.metrics.event to take over.");
        }
    }

    @Override
    public void close() {
    }
}
