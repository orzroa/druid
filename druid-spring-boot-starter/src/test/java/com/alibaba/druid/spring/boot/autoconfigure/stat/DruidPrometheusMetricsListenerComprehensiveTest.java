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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * 针对 {@link DruidPrometheusMetricsListener} 的综合单元测试集合。
 *
 * <p>测试覆盖范围：
 * <ul>
 *     <li>SQL 事件：执行耗时、影响行数、抓取行数的记录与跳过条件；</li>
 *     <li>Web 事件：URI 模板解析的三级优先级、contextPath 拼接、JDBC 计数；</li>
 *     <li>基数控制：SQL / URI 身份上限的 LRU 淘汰；</li>
 *     <li>SQL 映射落盘：MD5 文件名、已存在文件不覆盖、关闭后不落盘；</li>
 *     <li>配置热刷新：{@code refresh} 启用/禁用、null 忽略；</li>
 *     <li>数据源名解析：getName / BeanFactory 反查 / unknown；</li>
 *     <li>生命周期：无 MeterRegistry 时不报错、destroy 幂等；</li>
 *     <li>并发：队列满时丢弃且不阻塞业务事件路径。</li>
 * </ul>
 *
 * <p>本测试不修改任何生产代码，仅通过公共 API 与 Micrometer 检索断言行为。
 */
public class DruidPrometheusMetricsListenerComprehensiveTest {
    private static final String MVC_PATTERN_KEY =
            "org.springframework.web.servlet.HandlerMapping.bestMatchingPattern";

    private final List<DruidPrometheusMetricsListener> listeners = new ArrayList<DruidPrometheusMetricsListener>();

    @After
    public void tearDown() {
        for (DruidPrometheusMetricsListener listener : listeners) {
            try {
                listener.destroy();
            } catch (RuntimeException ignored) {
                // 忽略销毁过程中的异常，确保一个测试的失败不影响后续清理
            }
        }
        listeners.clear();
    }

    // ==================== calculateSqlMd5 ====================

    @Test
    public void sqlMd5_emptyOrBlankReturnsEmptyString() {
        assertEquals("", DruidPrometheusMetricsListener.calculateSqlMd5(null));
        assertEquals("", DruidPrometheusMetricsListener.calculateSqlMd5(""));
        assertEquals("", DruidPrometheusMetricsListener.calculateSqlMd5("   "));
        assertEquals("", DruidPrometheusMetricsListener.calculateSqlMd5("\t\n"));
    }

    @Test
    public void sqlMd5_isLowercaseHexOfLength32() {
        String hash = DruidPrometheusMetricsListener.calculateSqlMd5("select 1");
        assertEquals(32, hash.length());
        assertTrue(hash.matches("[0-9a-f]{32}"));
    }

    @Test
    public void sqlMd5_matchesStandardMessageDigest() throws Exception {
        String sql = "SELECT * FROM users WHERE id = ?";
        byte[] expected = MessageDigest.getInstance("MD5").digest(sql.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(32);
        char[] chars = "0123456789abcdef".toCharArray();
        for (byte b : expected) {
            int u = b & 0xff;
            hex.append(chars[u >>> 4]).append(chars[u & 0x0f]);
        }
        assertEquals(hex.toString(), DruidPrometheusMetricsListener.calculateSqlMd5(sql));
    }

    @Test
    public void sqlMd5_deterministicAndDistinct() {
        String a = DruidPrometheusMetricsListener.calculateSqlMd5("select 1");
        String b = DruidPrometheusMetricsListener.calculateSqlMd5("select 1");
        String c = DruidPrometheusMetricsListener.calculateSqlMd5("select 2");
        assertEquals(a, b);
        assertFalse(a.equals(c));
    }

    // ==================== SQL 事件 ====================

    @Test
    public void onSqlExecute_recordsDurationAsTimer() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy ds = dataSource("primary");

        listener.onSqlExecute("select 1", ds, 1_000_000L, null);

        Timer timer = registry.find("druid.sql.execution.duration")
                .tags("sql", hash("select 1"), "datasource", "primary").timer();
        assertNotNull(timer);
        // Meter 同步创建，首次执行即被记录
        assertEquals(1L, timer.count());
        assertEquals(1_000_000.0, timer.totalTime(TimeUnit.NANOSECONDS), 0.0);
    }

    @Test
    public void onSqlExecute_reusesSameMeterForSameSql() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy ds = dataSource("primary");

        listener.onSqlExecute("select 1", ds, 100L, null);
        listener.onSqlExecute("select 1", ds, 200L, null);
        listener.onSqlExecute("select 1", ds, 300L, null);

        Timer timer = registry.find("druid.sql.execution.duration")
                .tags("sql", hash("select 1"), "datasource", "primary").timer();
        // 复用同一 Meter，三次记录都计入（首次不再丢失）
        assertEquals(3L, timer.count());
        assertEquals(600.0, timer.totalTime(TimeUnit.NANOSECONDS), 0.0);
    }

    @Test
    public void onSqlExecute_sameSqlDifferentDataSourceCreatesDistinctMeters() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy dsA = dataSource("ds-a");
        DataSourceProxy dsB = dataSource("ds-b");

        listener.onSqlExecute("select 1", dsA, 100L, null);
        listener.onSqlExecute("select 1", dsB, 200L, null);
        awaitSqlMeter(registry, "select 1", "ds-a");
        awaitSqlMeter(registry, "select 1", "ds-b");

        assertEquals(2, registry.find("druid.sql.execution.duration").meters().size());
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", hash("select 1"), "datasource", "ds-a").timer());
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", hash("select 1"), "datasource", "ds-b").timer());
    }

    @Test
    public void onSqlExecute_skipsFailedSql() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();

        listener.onSqlExecute("select broken", dataSource("primary"), 100L, new IllegalStateException("boom"));
        Thread.sleep(50L);

        assertEquals(0, registry.find("druid.sql.execution.duration").meters().size());
    }

    @Test
    public void onSqlExecute_skipsNullOrEmptySql() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy ds = dataSource("primary");

        listener.onSqlExecute(null, ds, 100L, null);
        listener.onSqlExecute("", ds, 100L, null);
        Thread.sleep(50L);

        assertEquals(0, registry.find("druid.sql.execution.duration").meters().size());
    }

    @Test
    public void onSqlUpdateCount_recordsAffectedRowsAndSkipsNegative() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy ds = dataSource("primary");

        listener.onSqlExecute("update t set x=1", ds, 100L, null);
        awaitSqlMeter(registry, "update t set x=1", "primary");

        listener.onSqlUpdateCount("update t set x=1", ds, -1); // 未知，不记录
        listener.onSqlUpdateCount("update t set x=1", ds, 5);
        listener.onSqlUpdateCount("update t set x=1", ds, 7);

        DistributionSummary summary = registry.find("druid.sql.affected.rows")
                .tags("sql", hash("update t set x=1"), "datasource", "primary").summary();
        assertNotNull(summary);
        assertEquals(2L, summary.count());
        assertEquals(12.0, summary.totalAmount(), 0.0);
    }

    @Test
    public void onSqlResultSetClose_recordsFetchedRows() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy ds = dataSource("primary");

        listener.onSqlExecute("select * from t", ds, 100L, null);
        awaitSqlMeter(registry, "select * from t", "primary");
        listener.onSqlResultSetClose("select * from t", ds, 42);

        DistributionSummary summary = registry.find("druid.sql.fetched.rows")
                .tags("sql", hash("select * from t"), "datasource", "primary").summary();
        assertNotNull(summary);
        assertEquals(1L, summary.count());
        assertEquals(42.0, summary.totalAmount(), 0.0);
    }

    // ==================== SQL 身份上限 ====================

    @Test
    public void sqlIdentityLimit_evictsLeastRecentlyUsedSql() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = defaultConfig(false);
        config.getEvents().setMaxSqlIdentities(2);
        DruidPrometheusMetricsListener listener = newListener(config, registry, null);
        listener.init();
        DataSourceProxy ds = dataSource("primary");

        listener.onSqlExecute("select 1", ds, 1L, null);
        listener.onSqlExecute("select 2", ds, 1L, null);
        listener.onSqlExecute("select 1", ds, 1L, null); // 刷新 select 1 的访问顺序
        listener.onSqlExecute("select 3", ds, 1L, null); // 淘汰最久未使用的 select 2

        awaitSqlMeter(registry, "select 1", "primary");
        awaitSqlMeter(registry, "select 3", "primary");

        assertEquals(2, registry.find("druid.sql.execution.duration").meters().size());
        assertEquals(null, registry.find("druid.sql.execution.duration")
                .tags("sql", hash("select 2"), "datasource", "primary").timer());
    }

    // ==================== SQL 映射落盘 ====================

    @Test
    public void sqlMapping_writesMd5NamedFile() throws Exception {
        Path dir = Files.createTempDirectory("druid-prom-mapping-");
        try {
            String sql = "select 2";
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            DruidStatProperties.Prometheus config = defaultConfig(true);
            config.getSqlMapping().setDirectory(dir.toString());
            DruidPrometheusMetricsListener listener = newListener(config, registry, null);
            listener.init();

            listener.onSqlExecute(sql, dataSource("primary"), 1L, null);

            Path file = dir.resolve(hash(sql) + ".sql");
            awaitFile(file); // Meter 同步创建，但文件落盘异步
            assertEquals(sql, new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    public void sqlMapping_doesNotOverwriteExistingFile() throws Exception {
        Path dir = Files.createTempDirectory("druid-prom-mapping-");
        try {
            String sql = "select 3";
            String hash = hash(sql);
            Files.write(dir.resolve(hash + ".sql"), "ORIGINAL".getBytes(StandardCharsets.UTF_8));

            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            DruidStatProperties.Prometheus config = defaultConfig(true);
            config.getSqlMapping().setDirectory(dir.toString());
            DruidPrometheusMetricsListener listener = newListener(config, registry, null);
            listener.init();

            listener.onSqlExecute(sql, dataSource("primary"), 1L, null);
            awaitSqlMeter(registry, sql, "primary");

            assertEquals("ORIGINAL",
                    new String(Files.readAllBytes(dir.resolve(hash + ".sql")), StandardCharsets.UTF_8));
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    public void sqlMapping_disabledDoesNotWriteFile() throws Exception {
        Path dir = Files.createTempDirectory("druid-prom-mapping-");
        try {
            String sql = "select 4";
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            DruidStatProperties.Prometheus config = defaultConfig(false); // mapping disabled
            config.getSqlMapping().setDirectory(dir.toString());
            DruidPrometheusMetricsListener listener = newListener(config, registry, null);
            listener.init();

            listener.onSqlExecute(sql, dataSource("primary"), 1L, null);
            awaitSqlMeter(registry, sql, "primary");

            assertFalse(Files.exists(dir.resolve(hash(sql) + ".sql")));
        } finally {
            deleteRecursively(dir);
        }
    }

    // ==================== Web 事件 ====================

    @Test
    public void onWebRequest_recordsDurationAndJdbcCounts() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();

        listener.onWebRequest(request(null, ""), "/orders/1", 5_000_000L, 2, 3, 4, null);

        Timer timer = registry.find("druid.uri.request.duration").tag("uri", "/orders/1").timer();
        assertNotNull(timer);
        assertEquals(1L, timer.count());
        // 5_000_000 ns
        assertEquals(5_000_000.0, timer.totalTime(TimeUnit.NANOSECONDS), 0.0);
        assertEquals(2.0, registry.find("druid.uri.jdbc.executions").tag("uri", "/orders/1").summary().totalAmount(), 0.0);
        assertEquals(3.0, registry.find("druid.uri.jdbc.affected.rows").tag("uri", "/orders/1").summary().totalAmount(), 0.0);
        assertEquals(4.0, registry.find("druid.uri.jdbc.fetched.rows").tag("uri", "/orders/1").summary().totalAmount(), 0.0);
    }

    @Test
    public void onWebRequest_negativeJdbcCountsNotRecorded() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();

        listener.onWebRequest(request(null, ""), "/orders/1", 1L, -1, -1, -1, null);

        Timer timer = registry.find("druid.uri.request.duration").tag("uri", "/orders/1").timer();
        assertNotNull(timer);
        assertEquals(1L, timer.count());
        // Meter 在首次见到 URI 时即注册，但负值不会产生观测
        DistributionSummary jdbcExec = registry.find("druid.uri.jdbc.executions")
                .tag("uri", "/orders/1").summary();
        assertNotNull(jdbcExec);
        assertEquals(0L, jdbcExec.count());
        assertEquals(0.0, jdbcExec.totalAmount(), 0.0);
    }

    @Test
    public void onWebRequest_skipsEmptyUriTemplate() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();

        listener.onWebRequest(request(null, ""), "", 1L, 0, 0, 0, null);
        assertEquals(0, registry.find("druid.uri.request.duration").meters().size());
    }

    @Test
    public void uriTemplate_resolverSpiWinsOverMvcPatternAndFallback() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidUriTemplateResolver resolver = request1 -> "/api/{id}";
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, resolver);
        listener.init();

        listener.onWebRequest(request("/orders/{id}", "/orders/1"), "/orders/1", 1L, 0, 0, 0, null);

        assertNotNull(registry.find("druid.uri.request.duration").tag("uri", "/api/{id}").timer());
        assertEquals(0, registry.find("druid.uri.request.duration").tag("uri", "/orders/{id}").meters().size());
    }

    @Test
    public void uriTemplate_mvcPatternUsedWhenResolverAbsent() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();

        listener.onWebRequest(request("/orders/{id}", ""), "/orders/1", 1L, 0, 0, 0, null);

        assertNotNull(registry.find("druid.uri.request.duration").tag("uri", "/orders/{id}").timer());
    }

    @Test
    public void uriTemplate_rawUriUsedAsFallback() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();

        listener.onWebRequest(request(null, ""), "/orders/1", 1L, 0, 0, 0, null);

        assertNotNull(registry.find("druid.uri.request.duration").tag("uri", "/orders/1").timer());
    }

    @Test
    public void uriTemplate_includeContextPathPrependsContextPath() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = defaultConfig(false);
        config.getUriTemplate().setIncludeContextPath(true);
        DruidPrometheusMetricsListener listener = newListener(config, registry, null);
        listener.init();

        listener.onWebRequest(request(null, "/app"), "/orders/1", 1L, 0, 0, 0, null);

        assertNotNull(registry.find("druid.uri.request.duration").tag("uri", "/app/orders/1").timer());
    }

    @Test
    public void uriIdentityLimit_evictsLeastRecentlyUsedUri() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = defaultConfig(false);
        config.getEvents().setMaxUriIdentities(1);
        DruidPrometheusMetricsListener listener = newListener(config, registry, null);
        listener.init();

        listener.onWebRequest(request(null, ""), "/one", 1L, 0, 0, 0, null);
        listener.onWebRequest(request(null, ""), "/two", 1L, 0, 0, 0, null);

        assertEquals(1, registry.find("druid.uri.request.duration").meters().size());
        assertEquals(null, registry.find("druid.uri.request.duration").tag("uri", "/one").timer());
        assertNotNull(registry.find("druid.uri.request.duration").tag("uri", "/two").timer());
    }

    // ==================== 配置热刷新 ====================

    @Test
    public void refresh_nullIsIgnored() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus disabled = defaultConfig(false);
        disabled.getEvents().setEnabled(false);
        DruidPrometheusMetricsListener listener = newListener(disabled, registry, null);
        listener.init();

        listener.refresh(null); // 忽略
        listener.onWebRequest(request(null, ""), "/orders", 1L, 0, 0, 0, null);
        assertEquals(0, registry.find("druid.uri.request.duration").meters().size());
    }

    @Test
    public void refresh_identityLimitTrimsLruOnNextEvent() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = defaultConfig(false);
        config.getEvents().setMaxUriIdentities(2);
        DruidPrometheusMetricsListener listener = newListener(config, registry, null);
        listener.init();

        listener.onWebRequest(request(null, ""), "/one", 1L, 0, 0, 0, null);
        listener.onWebRequest(request(null, ""), "/two", 1L, 0, 0, 0, null);

        DruidStatProperties.Prometheus refreshed = defaultConfig(false);
        refreshed.getEvents().setMaxUriIdentities(1);
        listener.refresh(refreshed);
        listener.onWebRequest(request(null, ""), "/two", 1L, 0, 0, 0, null);

        assertEquals(1, registry.find("druid.uri.request.duration").meters().size());
        assertEquals(null, registry.find("druid.uri.request.duration").tag("uri", "/one").timer());
        assertNotNull(registry.find("druid.uri.request.duration").tag("uri", "/two").timer());
    }

    @Test
    public void refresh_enablesSqlEventsAfterDisabled() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus disabled = defaultConfig(false);
        disabled.setEnabled(false);
        DruidPrometheusMetricsListener listener = newListener(disabled, registry, null);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 1L, null);
        Thread.sleep(50L);
        assertEquals(0, registry.find("druid.sql.execution.duration").meters().size());

        DruidStatProperties.Prometheus enabled = defaultConfig(false);
        listener.refresh(enabled);
        listener.onSqlExecute("select 1", dataSource("primary"), 1L, null);
        awaitSqlMeter(registry, "select 1", "primary");
    }

    // ==================== 数据源名解析 ====================

    @Test
    public void dataSourceName_unknownWhenNoNameAndNoBeanFactory() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy ds = mock(DataSourceProxy.class); // getName() 默认 null

        listener.onSqlExecute("select 1", ds, 1L, null);
        awaitSqlMeter(registry, "select 1", "unknown");
    }

    @Test
    public void dataSourceName_resolvedFromBeanFactory() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        // getName() 为 null 且同时实现 javax.sql.DataSource，才能走到 BeanFactory 反查分支
        DataSourceProxy ds = mock(DataSourceProxy.class, withSettings().extraInterfaces(DataSource.class));
        beanFactory.registerSingleton("inventoryDataSource", ds);
        listener.setBeanFactory(beanFactory);
        listener.init();

        listener.onSqlExecute("select 1", ds, 1L, null);
        awaitSqlMeter(registry, "select 1", "inventoryDataSource");
    }

    @Test
    public void dataSourceName_usesDatabaseFromJdbcUrl() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        DataSourceProxy ds = mock(DataSourceProxy.class);
        when(ds.getUrl()).thenReturn("jdbc:mysql://10.108.0.203:3306/crs?useSSL=false");

        listener.onSqlExecute("select 1", ds, 1L, null);
        // 库名 crs 取自 URL 路径，且已剥离查询参数
        awaitSqlMeter(registry, "select 1", "crs");
    }

    // ==================== 生命周期 ====================

    @Test
    public void init_withoutMeterRegistryIsNoOpAndDoesNotThrow() {
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), null, null);
        listener.init(); // 无 MeterRegistry，应直接返回

        // 事件路径不应产生 NPE 或注册任何 Meter
        listener.onSqlExecute("select 1", dataSource("primary"), 1L, null);
        listener.onWebRequest(request(null, ""), "/orders", 1L, 0, 0, 0, null);
    }

    @Test
    public void init_doesNotRegisterDroppedCounters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();

        assertEquals(0, registry.find("druid.prometheus.meter.dropped").meters().size());
    }

    @Test
    public void configuration_registersListenerWhenEnabledPropertyIsMissing() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(MetricsConfiguration.class, DruidPrometheusMetricsConfiguration.class);
        context.refresh();
        try {
            assertTrue(context.containsBean("druidPrometheusMetricsListener"));
        } finally {
            context.close();
        }
    }

    @Test
    public void destroy_isIdempotent() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(defaultConfig(false), registry, null);
        listener.init();
        listener.destroy();
        listener.destroy(); // 第二次不应抛异常
    }

    // ==================== 并发：队列满丢弃 ====================

    @Test
    public void queueFull_doesNotLoseMetricsUnderWritePressure() throws Exception {
        Path dir = Files.createTempDirectory("druid-prom-queue-");
        try {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            DruidStatProperties.Prometheus config = defaultConfig(true);
            config.getSqlMapping().setDirectory(dir.toString());
            config.getSqlMapping().setQueueSize(1);
            config.getEvents().setMaxSqlIdentities(10_000);
            DruidPrometheusMetricsListener listener = newListener(config, registry, null);
            listener.init();

            final int total = 300;
            // 单线程顺序提交大量不同 SQL，落盘单写线程无法及时消费；
            // 但 Meter 同步创建，队列满只会跳过文件落盘，不应丢失任何观测
            for (int i = 0; i < total; i++) {
                listener.onSqlExecute("select " + i, dataSource("primary"), 1L, null);
            }

            // 全部 SQL 都应建出 Meter，无丢弃
            awaitCondition(5_000L, () -> registry.find("druid.sql.execution.duration").meters().size() == total);
            assertEquals(total, registry.find("druid.sql.execution.duration").meters().size());
        } finally {
            deleteRecursively(dir);
        }
    }

    // ==================== 工具方法 ====================

    private DruidPrometheusMetricsListener newListener(DruidStatProperties.Prometheus config,
                                                       MeterRegistry registry,
                                                       DruidUriTemplateResolver resolver) {
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(
                config, provider(registry), provider(resolver));
        listeners.add(listener);
        return listener;
    }

    private static DruidStatProperties.Prometheus defaultConfig(boolean mappingEnabled) {
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getSqlMapping().setEnabled(mappingEnabled);
        return config;
    }

    private static DataSourceProxy dataSource(String name) {
        DataSourceProxy ds = mock(DataSourceProxy.class);
        when(ds.getName()).thenReturn(name);
        return ds;
    }

    /** 构造一个 request mock：mvcPattern 为 null 表示无 MVC 模板属性；contextPath 为回退上下文路径。 */
    private static HttpServletRequest request(String mvcPattern, String contextPath) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getAttribute(MVC_PATTERN_KEY)).thenReturn(mvcPattern);
        when(request.getContextPath()).thenReturn(contextPath);
        return request;
    }

    private static String hash(String sql) {
        return DruidPrometheusMetricsListener.calculateSqlMd5(sql);
    }

    private static void awaitSqlMeter(MeterRegistry registry, String sql, String dataSource) throws Exception {
        String hash = hash(sql);
        awaitCondition(3_000L, () -> registry.find("druid.sql.execution.duration")
                .tags("sql", hash, "datasource", dataSource).timer() != null);
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", hash, "datasource", dataSource).timer());
    }

    private static void awaitFile(Path file) throws Exception {
        awaitCondition(3_000L, () -> Files.exists(file));
        assertTrue(Files.exists(file));
    }

    private static void awaitCondition(long timeoutMs, BooleanSupplier condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue("条件在 " + timeoutMs + "ms 内未满足", condition.getAsBoolean());
    }

    private static void deleteRecursively(Path path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try {
            if (Files.isDirectory(path)) {
                for (Path child : Files.newDirectoryStream(path)) {
                    deleteRecursively(child);
                }
            }
            Files.deleteIfExists(path);
        } catch (Exception ignored) {
            // 测试清理失败不影响断言
        }
    }

    private static <T> ObjectProvider<T> provider(final T value) {
        return new ObjectProvider<T>() {
            @Override public T getObject(Object... args) throws BeansException { return value; }
            @Override public T getIfAvailable() throws BeansException { return value; }
            @Override public T getIfUnique() throws BeansException { return value; }
            @Override public T getObject() throws BeansException { return value; }
        };
    }

    @Configuration
    static class MetricsConfiguration {
        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        public DruidStatProperties druidStatProperties() {
            return new DruidStatProperties();
        }
    }
}
