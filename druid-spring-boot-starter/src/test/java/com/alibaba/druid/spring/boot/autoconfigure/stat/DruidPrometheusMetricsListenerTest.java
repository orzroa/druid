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

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;

import javax.servlet.http.HttpServletRequest;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DruidPrometheusMetricsListenerTest {
    @Test
    public void testSqlMd5MatchesDruid2Prom() {
        assertEquals("f15e5e09c27c92be6ed2b586d171d68a",
                DruidPrometheusMetricsListener.calculateSqlMd5("SELECT * FROM users WHERE id = ?"));
    }

    @Test
    public void testUriMetersRecordAllValues() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = listener(registry);
        listener.init();
        listener.onWebRequest(request("/orders/{id}"), "/orders/1", 1000000L, 2, 3, 4, null);

        assertNotNull(registry.find("druid.uri.request.duration").tag("uri", "/orders/{id}").timer());
        assertEquals(1L, registry.find("druid.uri.request.duration").tag("uri", "/orders/{id}").timer().count());
        assertEquals(2L, registry.find("druid.uri.jdbc.executions").tag("uri", "/orders/{id}").summary().totalAmount(), 0);
        listener.destroy();
    }

    @Test
    public void testSqlMeterIsCreatedAfterMappingTask() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = listener(registry);
        listener.init();
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setName("primary");
        listener.onSqlExecute("select 1", dataSource, 1000L, null);
        awaitSqlMeter(registry, "select 1", "primary");
        listener.onSqlExecute("select 1", dataSource, 1000L, null);
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5("select 1"), "datasource", "primary").timer());
        listener.destroy();
    }

    @Test
    public void testFailedSqlDoesNotCreateMeter() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = listener(registry);
        listener.init();
        listener.onSqlExecute("select broken", null, 1000L, new IllegalStateException("failure"));
        Thread.sleep(20L);
        assertEquals(0, registry.find("druid.sql.execution.duration").meters().size());
        listener.destroy();
    }

    @Test
    public void testUriIdentityLimitDropsOnlyNewUri() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getEvents().setMaxUriIdentities(1);
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(config, provider(registry),
                provider((DruidUriTemplateResolver) null));
        listener.init();
        listener.onWebRequest(request("/one"), "/one", 1L, 0, 0, 0, null);
        listener.onWebRequest(request("/two"), "/two", 1L, 0, 0, 0, null);
        assertEquals(1, registry.find("druid.uri.request.duration").meters().size());
        assertEquals(1L, registry.find("druid.prometheus.meter.dropped").tag("type", "uri").counter().count(), 0);
        listener.destroy();
    }

    @Test
    public void testRefreshEnablesSubsequentEvents() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus disabled = new DruidStatProperties.Prometheus();
        disabled.getEvents().setEnabled(false);
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(disabled, provider(registry),
                provider((DruidUriTemplateResolver) null));
        listener.init();
        listener.onWebRequest(request("/orders"), "/orders", 1L, 0, 0, 0, null);
        DruidStatProperties.Prometheus enabled = new DruidStatProperties.Prometheus();
        listener.refresh(enabled);
        listener.onWebRequest(request("/orders"), "/orders", 1L, 0, 0, 0, null);
        assertEquals(1L, registry.find("druid.uri.request.duration").tag("uri", "/orders").timer().count());
        listener.destroy();
    }

    @Test
    public void testSqlMappingUsesMd5FileName() throws Exception {
        Path directory = Files.createTempDirectory("druid-prometheus-test-");
        String sql = "select 2";
        String hash = DruidPrometheusMetricsListener.calculateSqlMd5(sql);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getSqlMapping().setDirectory(directory.toString());
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(config, provider(registry),
                provider((DruidUriTemplateResolver) null));
        listener.init();
        listener.onSqlExecute(sql, null, 1L, null);
        // Meter 同步创建，但 SQL 文本映射文件异步落盘，需等待文件写入完成
        Path mappingFile = directory.resolve(hash);
        long deadline = System.currentTimeMillis() + 2000L;
        while (!Files.exists(mappingFile) && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(sql, new String(Files.readAllBytes(mappingFile), StandardCharsets.UTF_8));
        listener.destroy();
        Files.deleteIfExists(directory.resolve(hash));
        Files.deleteIfExists(directory);
    }

    private static void awaitSqlMeter(SimpleMeterRegistry registry, String sql, String dataSource) throws Exception {
        long deadline = System.currentTimeMillis() + 2000L;
        while (registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5(sql), "datasource", dataSource)
                .timer() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5(sql), "datasource", dataSource).timer());
    }

    private static DruidPrometheusMetricsListener listener(SimpleMeterRegistry registry) {
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getSqlMapping().setEnabled(false);
        return new DruidPrometheusMetricsListener(config, provider(registry), provider((DruidUriTemplateResolver) null));
    }

    private static HttpServletRequest request(final String pattern) {
        return (HttpServletRequest) Proxy.newProxyInstance(HttpServletRequest.class.getClassLoader(),
                new Class[] {HttpServletRequest.class}, (proxy, method, args) -> {
                    if ("getAttribute".equals(method.getName())) return pattern;
                    if ("getContextPath".equals(method.getName())) return "";
                    return null;
                });
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
