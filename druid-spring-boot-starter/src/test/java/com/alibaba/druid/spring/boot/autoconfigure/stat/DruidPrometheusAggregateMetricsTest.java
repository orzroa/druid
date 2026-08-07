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

import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DruidPrometheusAggregateMetricsTest {
    private final List<DruidPrometheusMetricsListener> listeners =
            new ArrayList<DruidPrometheusMetricsListener>();

    @After
    public void tearDown() {
        for (DruidPrometheusMetricsListener listener : listeners) listener.destroy();
    }

    @Test
    public void phaseTwoSqlAggregatesByDatasourceWhenPhaseOneIsDisabled() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = config();
        config.getEvents().setEnabled(false);
        DruidPrometheusMetricsListener listener = listener(config, registry, DruidMetricsEventSink.NOOP);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("orders"), 10L, null);
        listener.onSqlExecute("select 2", dataSource("orders"), 20L, new IllegalStateException("failed"));
        listener.onSqlExecute("select 3", dataSource("inventory"), 30L, null);

        Timer orders = registry.find("druid.agg.sql.execution.duration")
                .tag("datasource", "orders").timer();
        assertNotNull(orders);
        assertEquals(2L, orders.count());
        assertEquals(2, registry.find("druid.agg.sql.execution.duration").meters().size());
        assertEquals(0, registry.find("druid.sql.execution.duration").meters().size());
        assertNull(orders.getId().getTag("sql"));
    }

    @Test
    public void aggregateThresholdsUseGreaterThanOrEqualAndZeroDisables() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = config();
        config.getEvents().setEnabled(false);
        config.getThresholds().setSlowSqlMillis(10L);
        config.getThresholds().setLargeSqlReadRows(100L);
        config.getThresholds().setLargeSqlWriteRows(0L);
        DruidPrometheusMetricsListener listener = listener(config, registry, DruidMetricsEventSink.NOOP);
        listener.init();
        DataSourceProxy dataSource = dataSource("orders");

        listener.onSqlExecute("select ?", dataSource, TimeUnit.MILLISECONDS.toNanos(10L), null);
        listener.onSqlExecute("select failed", dataSource, TimeUnit.MILLISECONDS.toNanos(11L),
                new IllegalStateException("failed"));
        listener.onSqlResultSetClose("select ?", dataSource, 100);
        listener.onSqlUpdateCount("update t set c=?", dataSource, 1000);

        assertEquals(2D, counter(registry, "druid.agg.sql.slow", "orders").count(), 0D);
        assertEquals(1D, counter(registry, "druid.agg.sql.large.read", "orders").count(), 0D);
        assertEquals(0D, counter(registry, "druid.agg.sql.large.write", "orders").count(), 0D);
    }

    @Test
    public void uriMetricsHaveNoUriLabelAndCountAllUris() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = config();
        config.getEvents().setEnabled(false);
        config.getThresholds().setSlowUriMillis(10L);
        config.getThresholds().setLargeUriReadRows(5L);
        config.getThresholds().setLargeUriWriteRows(4L);
        config.getThresholds().setLargeUriSqlExecutions(3L);
        DruidPrometheusMetricsListener listener = listener(config, registry, DruidMetricsEventSink.NOOP);
        listener.init();

        listener.onWebRequest(request(), "/orders/1", TimeUnit.MILLISECONDS.toNanos(10), 3, 4, 5, null);
        listener.onWebRequest(request(), "/users/2", 1L, 0, 0, 0, null);

        Timer timer = registry.find("druid.agg.uri.request.duration").timer();
        assertNotNull(timer);
        assertEquals(2L, timer.count());
        assertNull(timer.getId().getTag("uri"));
        assertEquals(1D, registry.find("druid.agg.uri.slow").counter().count(), 0D);
        assertEquals(1D, registry.find("druid.agg.uri.large.read").counter().count(), 0D);
        assertEquals(1D, registry.find("druid.agg.uri.large.write").counter().count(), 0D);
        assertEquals(1D, registry.find("druid.agg.uri.large.sql.executions").counter().count(), 0D);
    }

    @Test
    public void globalSwitchStopsAndResumesExistingAggregateMeter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = config();
        config.getEvents().setEnabled(false);
        DruidPrometheusMetricsListener listener = listener(config, registry, DruidMetricsEventSink.NOOP);
        listener.init();
        DataSourceProxy dataSource = dataSource("orders");

        listener.onSqlExecute("select 1", dataSource, 1L, null);
        DruidStatProperties.Prometheus disabled = config();
        disabled.setEnabled(false);
        disabled.getEvents().setEnabled(false);
        listener.refresh(disabled);
        listener.onSqlExecute("select 2", dataSource, 1L, null);
        DruidStatProperties.Prometheus enabled = config();
        enabled.getEvents().setEnabled(false);
        listener.refresh(enabled);
        listener.onSqlExecute("select 3", dataSource, 1L, null);

        assertEquals(2L, registry.find("druid.agg.sql.execution.duration")
                .tag("datasource", "orders").timer().count());
    }

    @Test
    public void emittedSqlEventWritesJsonAndMappingButUnsampledEventDoesNot() throws Exception {
        Path directory = Files.createTempDirectory("druid-phase2-events-");
        try {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            DruidStatProperties.Prometheus config = config();
            config.getEvents().setEnabled(false);
            config.getSqlMapping().setEnabled(false);
            config.getLogging().setEnabled(true);
            config.getLogging().setDirectory(directory.toString());
            config.getLogging().setNormalSampleRate(0D);
            config.getThresholds().setSlowSqlMillis(10L);
            RecordingSink sink = new RecordingSink();
            DruidPrometheusMetricsListener listener = listener(config, registry, sink);
            listener.init();

            listener.onSqlExecute("select normal", dataSource("orders"), 1L, null);
            String abnormalSql = "select slow\nfrom orders";
            listener.onSqlExecute(abnormalSql, dataSource("orders"),
                    TimeUnit.MILLISECONDS.toNanos(10L), null);

            String hash = DruidPrometheusMetricsListener.calculateSqlMd5(abnormalSql);
            Path mapping = directory.resolve("sql_mapping_" + hash + ".log");
            awaitFile(mapping);
            assertEquals(abnormalSql, new String(Files.readAllBytes(mapping), StandardCharsets.UTF_8));
            assertEquals(1, sink.events.size());
            assertTrue(sink.events.get(0).contains("\"event\":\"sql_execute\""));
            assertTrue(sink.events.get(0).contains("\"sql_md5\":\"" + hash + "\""));
            assertFalse(sink.events.get(0).contains("sql_template"));
            assertFalse(Files.exists(directory.resolve("sql_mapping_"
                    + DruidPrometheusMetricsListener.calculateSqlMd5("select normal") + ".log")));
        } finally {
            delete(directory);
        }
    }

    @Test
    public void eventJsonEscapesUriAndErrorsAreAlwaysLogged() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = config();
        config.getEvents().setEnabled(false);
        config.getLogging().setEnabled(true);
        config.getLogging().setNormalSampleRate(0D);
        RecordingSink sink = new RecordingSink();
        DruidPrometheusMetricsListener listener = listener(config, registry, sink);
        listener.init();

        listener.onWebRequest(request(), "/a\"b\n", 1L, 0, 0, 0, new RuntimeException("failed"));

        assertEquals(1, sink.events.size());
        assertTrue(sink.events.get(0).contains("\"uri\":\"/a\\\"b\\n\""));
        assertTrue(sink.events.get(0).contains("\"error\":true"));
    }

    @Test
    public void refreshRejectsNegativeThresholdAndSampleRateIsClamped() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = config();
        config.getThresholds().setSlowSqlMillis(10L);
        DruidPrometheusMetricsListener listener = listener(config, registry, DruidMetricsEventSink.NOOP);
        listener.init();
        DruidStatProperties.Prometheus refreshed = config();
        refreshed.getThresholds().setSlowSqlMillis(-1L);
        refreshed.getLogging().setNormalSampleRate(2D);

        listener.refresh(refreshed);

        assertEquals(10L, refreshed.getThresholds().getSlowSqlMillis());
        assertEquals(1D, refreshed.getLogging().getNormalSampleRate(), 0D);
    }

    @Test
    public void inPlaceConfigurationChangeIsObservedOnNextEvent() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = config();
        config.getEvents().setEnabled(false);
        config.getThresholds().setSlowSqlMillis(100L);
        DruidPrometheusMetricsListener listener = listener(config, registry, DruidMetricsEventSink.NOOP);
        listener.init();
        DataSourceProxy dataSource = dataSource("orders");

        listener.onSqlExecute("select 1", dataSource, TimeUnit.MILLISECONDS.toNanos(10L), null);
        config.getThresholds().setSlowSqlMillis(10L);
        listener.onSqlExecute("select 2", dataSource, TimeUnit.MILLISECONDS.toNanos(10L), null);

        assertEquals(1D, counter(registry, "druid.agg.sql.slow", "orders").count(), 0D);
    }

    @Test
    public void allGlobalPhaseOneAndLoggingSwitchCombinationsAreIndependent() throws Exception {
        Path directory = Files.createTempDirectory("druid-switch-matrix-");
        try {
            int index = 0;
            for (boolean global : new boolean[]{false, true}) {
                for (boolean phaseOne : new boolean[]{false, true}) {
                    for (boolean logging : new boolean[]{false, true}) {
                        SimpleMeterRegistry registry = new SimpleMeterRegistry();
                        DruidStatProperties.Prometheus config = config();
                        config.setEnabled(global);
                        config.getEvents().setEnabled(phaseOne);
                        config.getSqlMapping().setEnabled(false);
                        config.getLogging().setEnabled(logging);
                        config.getLogging().setNormalSampleRate(1D);
                        config.getLogging().setDirectory(directory.toString());
                        RecordingSink sink = new RecordingSink();
                        DruidPrometheusMetricsListener listener = listener(config, registry, sink);
                        listener.init();
                        String sql = "select matrix " + index++;

                        listener.onSqlExecute(sql, dataSource("orders"), 1L, null);

                        assertEquals(global && phaseOne ? 1 : 0,
                                registry.find("druid.sql.execution.duration").meters().size());
                        Timer aggregate = registry.find("druid.agg.sql.execution.duration")
                                .tag("datasource", "orders").timer();
                        assertEquals(global, aggregate != null && aggregate.count() == 1L);
                        assertEquals(global && logging ? 1 : 0, sink.events.size());
                        if (global && logging) {
                            awaitFile(directory.resolve("sql_mapping_"
                                    + DruidPrometheusMetricsListener.calculateSqlMd5(sql) + ".log"));
                        }
                        listener.destroy();
                    }
                }
            }
        } finally {
            delete(directory);
        }
    }

    private DruidPrometheusMetricsListener listener(DruidStatProperties.Prometheus config,
                                                     SimpleMeterRegistry registry,
                                                     DruidMetricsEventSink sink) {
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(
                config, provider(registry), provider((DruidUriTemplateResolver) null), sink);
        listeners.add(listener);
        return listener;
    }

    private static DruidStatProperties.Prometheus config() {
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getLogging().setEnabled(false);
        return config;
    }

    private static Counter counter(SimpleMeterRegistry registry, String name, String datasource) {
        return registry.find(name).tag("datasource", datasource).counter();
    }

    private static DataSourceProxy dataSource(String name) {
        DataSourceProxy dataSource = mock(DataSourceProxy.class);
        when(dataSource.getName()).thenReturn(name);
        return dataSource;
    }

    private static HttpServletRequest request() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getContextPath()).thenReturn("");
        return request;
    }

    private static void awaitFile(Path file) throws Exception {
        long deadline = System.currentTimeMillis() + 3000L;
        while (!Files.exists(file) && System.currentTimeMillis() < deadline) Thread.sleep(10L);
        assertTrue(Files.exists(file));
    }

    private static void delete(Path path) throws Exception {
        if (!Files.exists(path)) return;
        if (Files.isDirectory(path)) {
            java.nio.file.DirectoryStream<Path> children = Files.newDirectoryStream(path);
            try {
                for (Path child : children) delete(child);
            } finally {
                children.close();
            }
        }
        Files.deleteIfExists(path);
    }

    private static <T> ObjectProvider<T> provider(final T value) {
        return new ObjectProvider<T>() {
            @Override public T getObject(Object... args) throws BeansException { return value; }
            @Override public T getIfAvailable() throws BeansException { return value; }
            @Override public T getIfUnique() throws BeansException { return value; }
            @Override public T getObject() throws BeansException { return value; }
        };
    }

    private static final class RecordingSink implements DruidMetricsEventSink {
        private final List<String> events = new ArrayList<String>();
        @Override public void log(String json, boolean abnormal) { events.add(json); }
        @Override public boolean isActive() { return true; }
        @Override public boolean refresh(DruidStatProperties.Prometheus previous,
                                         DruidStatProperties.Prometheus current) { return true; }
        @Override public void close() { }
    }
}
