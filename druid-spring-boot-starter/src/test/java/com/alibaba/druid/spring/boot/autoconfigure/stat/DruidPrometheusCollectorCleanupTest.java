/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.spring.boot.autoconfigure.stat;

import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.prometheus.client.CollectorRegistry;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 验证 Micrometer 1.1.0 Prometheus 暴露层同步清理，不调用全局 CollectorRegistry.clear()。 */
public class DruidPrometheusCollectorCleanupTest {
    @Test
    public void removeDruidTimerRemovesPrometheusFamilyWithoutClearingOtherCollectors() {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM);
        Timer timer = Timer.builder("druid.sql.execution.duration")
                .tag("sql", "sql-hash")
                .tag("datasource", "primary")
                .register(meterRegistry);
        Timer jvmLikeTimer = Timer.builder("application.request.duration")
                .tag("uri", "/health")
                .register(meterRegistry);
        timer.record(1, java.util.concurrent.TimeUnit.MILLISECONDS);

        assertTrue(meterRegistry.scrape().contains("druid_sql_execution_duration"));
        assertTrue(meterRegistry.scrape().contains("application_request_duration"));

        DruidPrometheusCollectorCleanup cleanup = new DruidPrometheusCollectorCleanup(meterRegistry);
        assertNotNull(meterRegistry.remove(timer));
        assertTrue(cleanup.remove(timer));

        assertNull(meterRegistry.find("druid.sql.execution.duration")
                .tags("sql", "sql-hash", "datasource", "primary").timer());
        assertFalse("Druid family remains in Prometheus scrape", meterRegistry.scrape()
                .contains("druid_sql_execution_duration"));
        assertTrue(meterRegistry.scrape().contains("application_request_duration"));
        assertNotNull(meterRegistry.find("application.request.duration")
                .tags("uri", "/health").timer());
        assertNull(collectorRegistry.getSampleValue("druid_sql_execution_duration_count",
                new String[]{"datasource", "sql"}, new String[]{"primary", "sql-hash"}));
    }

    @Test
    public void removeDruidUriTimerRemovesUriPrometheusFamily() {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM);
        Timer timer = Timer.builder("druid.uri.request.duration")
                .tag("uri", "/users/{id}")
                .register(meterRegistry);
        assertTrue(meterRegistry.scrape().contains("druid_uri_request_duration"));

        DruidPrometheusCollectorCleanup cleanup = new DruidPrometheusCollectorCleanup(meterRegistry);
        assertNotNull(meterRegistry.remove(timer));
        assertTrue(cleanup.remove(timer));

        assertFalse("Druid URI family remains in Prometheus scrape", meterRegistry.scrape()
                .contains("druid_uri_request_duration"));
        assertNull(collectorRegistry.getSampleValue("druid_uri_request_duration_seconds_count",
                new String[]{"uri"}, new String[]{"/users/{id}"}));
    }

    @Test
    public void removeUsesConfiguredNamingConventionAndAllowsFamilyToBeRegisteredAgain() {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM);
        final NamingConvention delegate = meterRegistry.config().namingConvention();
        meterRegistry.config().namingConvention(new NamingConvention() {
            @Override
            public String name(String name, Meter.Type type, String baseUnit) {
                return "custom_" + delegate.name(name, type, baseUnit);
            }
        });
        Timer timer = Timer.builder("druid.uri.request.duration")
                .tag("uri", "/custom/{id}")
                .register(meterRegistry);
        timer.record(1, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(meterRegistry.scrape().contains("custom_druid_uri_request_duration"));

        assertNotNull(meterRegistry.remove(timer));
        assertTrue(new DruidPrometheusCollectorCleanup(meterRegistry).remove(timer));
        assertFalse(meterRegistry.scrape().contains("custom_druid_uri_request_duration"));

        Timer recreated = Timer.builder("druid.uri.request.duration")
                .tag("uri", "/custom/{id}")
                .register(meterRegistry);
        recreated.record(1, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue("family 清空后应能重新注册", meterRegistry.scrape()
                .contains("custom_druid_uri_request_duration"));
    }

    @Test
    public void removeDruidTimerFromCompositeRemovesPrometheusChild() {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        PrometheusMeterRegistry prometheus = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM);
        CompositeMeterRegistry composite = new CompositeMeterRegistry();
        composite.add(prometheus);
        Timer timer = Timer.builder("druid.sql.execution.duration")
                .tag("sql", "composite-sql")
                .tag("datasource", "primary")
                .register(composite);
        assertTrue(prometheus.scrape().contains("druid_sql_execution_duration"));

        assertNotNull(composite.remove(timer));
        assertTrue(new DruidPrometheusCollectorCleanup(composite).remove(timer));

        assertFalse(prometheus.scrape().contains("druid_sql_execution_duration"));
    }

    @Test
    public void prometheusMvcEndpointNoLongerExposesRemovedDruidMeter() throws Exception {
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM);
        Timer timer = Timer.builder("druid.sql.endpoint.cleanup.duration")
                .tag("sql", "endpoint-sql")
                .tag("datasource", "primary")
                .register(meterRegistry);
        timer.record(1, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(prometheusEndpointScrape().contains("druid_sql_endpoint_cleanup_duration"));

        assertNotNull(meterRegistry.remove(timer));
        assertTrue(new DruidPrometheusCollectorCleanup(meterRegistry).remove(timer));

        assertFalse("/admin/prometheus 仍暴露已删除的 Druid Meter",
                prometheusEndpointScrape().contains("druid_sql_endpoint_cleanup_duration"));
    }

    @Test
    public void sqlLruEvictionRemovesOnlyEvictedPrometheusLabelSet() {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM);
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getEvents().setMaxSqlIdentities(2);
        config.getEvents().getCleanup().setIntervalHours(0);
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(
                config, provider(meterRegistry), provider((DruidUriTemplateResolver) null));
        listener.init();
        try {
            DataSourceProxy dataSource = mock(DataSourceProxy.class);
            when(dataSource.getName()).thenReturn("primary");
            String first = DruidPrometheusMetricsListener.calculateSqlMd5("select 1");
            String second = DruidPrometheusMetricsListener.calculateSqlMd5("select 2");
            String third = DruidPrometheusMetricsListener.calculateSqlMd5("select 3");

            listener.onSqlExecute("select 1", dataSource, 1L, null);
            listener.onSqlExecute("select 2", dataSource, 1L, null);
            assertTrue(meterRegistry.scrape().contains(first));
            listener.onSqlExecute("select 3", dataSource, 1L, null);

            String scrape = meterRegistry.scrape();
            assertFalse("LRU 淘汰的 SQL 仍在 Prometheus 暴露层", scrape.contains(first));
            assertTrue(scrape.contains(second));
            assertTrue(scrape.contains(third));
        } finally {
            listener.destroy();
        }
    }

    @Test
    public void uriLruEvictionRemovesOnlyEvictedPrometheusLabelSet() {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(
                PrometheusConfig.DEFAULT, collectorRegistry, Clock.SYSTEM);
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getEvents().setMaxUriIdentities(2);
        config.getEvents().getCleanup().setIntervalHours(0);
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(
                config, provider(meterRegistry), provider((DruidUriTemplateResolver) null));
        listener.init();
        try {
            listener.onWebRequest(request("/first/{id}"), "/first/1", 1L, 0, 0, 0, null);
            listener.onWebRequest(request("/second/{id}"), "/second/2", 1L, 0, 0, 0, null);
            assertTrue(meterRegistry.scrape().contains("/first/{id}"));
            listener.onWebRequest(request("/third/{id}"), "/third/3", 1L, 0, 0, 0, null);

            String scrape = meterRegistry.scrape();
            assertFalse("LRU 淘汰的 URI 仍在 Prometheus 暴露层", scrape.contains("/first/{id}"));
            assertTrue(scrape.contains("/second/{id}"));
            assertTrue(scrape.contains("/third/{id}"));
        } finally {
            listener.destroy();
        }
    }

    private static HttpServletRequest request(String template) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getAttribute("org.springframework.web.servlet.HandlerMapping.bestMatchingPattern"))
                .thenReturn(template);
        return request;
    }

    /** 通过 simpleclient_spring_boot 0.5.0 的实际 MVC endpoint 抓取 defaultRegistry。 */
    private static String prometheusEndpointScrape() throws Exception {
        Class<?> endpointType = Class.forName("io.prometheus.client.spring.boot.PrometheusEndpoint");
        Constructor<?> endpointConstructor = endpointType.getDeclaredConstructor(CollectorRegistry.class);
        endpointConstructor.setAccessible(true);
        Object endpoint = endpointConstructor.newInstance(CollectorRegistry.defaultRegistry);

        Class<?> mvcType = Class.forName("io.prometheus.client.spring.boot.PrometheusMvcEndpoint");
        Constructor<?> mvcConstructor = mvcType.getConstructor(endpointType);
        Object mvcEndpoint = mvcConstructor.newInstance(endpoint);
        Method value = mvcType.getMethod("value", java.util.Set.class);
        ResponseEntity<?> response = (ResponseEntity<?>) value.invoke(mvcEndpoint, Collections.emptySet());
        return String.valueOf(response.getBody());
    }

    private static <T> ObjectProvider<T> provider(final T value) {
        return new ObjectProvider<T>() {
            @Override public T getObject(Object... args) throws BeansException { return value; }
            @Override public T getIfAvailable() throws BeansException { return value; }
            @Override public T getIfUnique() throws BeansException { return value; }
            @Override public T getObject() throws BeansException { return value; }
        };
    }
}
