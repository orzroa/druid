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
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;

import javax.servlet.http.HttpServletRequest;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 1.5 期高基数明细 Meter 定期清理的单元测试。
 *
 * <p>覆盖范围（对照 README-Prometheus-Metrics-Design-Phase1.5.md 验收项）：
 * <ul>
 *     <li>进程启动首条事件不立即清理，仅初始化基准时间；</li>
 *     <li>同固定清理点内不重复清理，跨固定清理点触发清理；</li>
 *     <li>跨自然日触发清理；</li>
 *     <li>n&lt;=0 关闭；n&gt;=24 退化为每日清理一次；</li>
 *     <li>清理后旧 Meter 从 registry 注销，identity/LRU 清空；</li>
 *     <li>触发事件清理后正常注册新 Meter；</li>
 *     <li>动态刷新：interval-hours 从合法变 0 关闭；</li>
 *     <li>不创建定时任务/后台线程。</li>
 * </ul>
 */
public class DruidMeterCleanupTest {
    private final List<DruidPrometheusMetricsListener> listeners = new ArrayList<DruidPrometheusMetricsListener>();

    @After
    public void tearDown() {
        for (DruidPrometheusMetricsListener listener : listeners) {
            try { listener.destroy(); } catch (RuntimeException ignored) { }
        }
        listeners.clear();
    }

    // ==================== 启动与基准时间初始化 ====================

    @Test
    public void startup_firstEventDoesNotCleanupOnlyInitializesBaseline() {
        // 10:00 启动，n=3，首条事件不应清理，仅初始化基准时间
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        // 先制造一些已有 Meter（模拟清理目标存在）——但这是首条事件，不应清理
        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);

        // 首条事件后应已创建 Meter，但没有被清理
        assertNotNull(registry.find("druid.sql.execution.duration").timer());
        // 10:30 仍在同一固定清理点 [9:00, 12:00)，不清理
        clock.advance(30, TimeUnit.MINUTES);
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);
        assertEquals(2, registry.find("druid.sql.execution.duration").meters().size());
    }

    // ==================== 固定清理点触发 ====================

    @Test
    public void crossBoundary_triggersCleanup() {
        // n=3，10:00 启动，到 12:05 跨入新区间应清理
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());

        // 推进到 12:05，跨越 12:00 固定清理点
        clock.setInstant(Instant.parse("2026-08-07T12:05:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);

        // 旧 Meter 应被清理，新 SQL 创建新 Meter
        // 清理后 select 1 的 Meter 注销，select 2 的新建
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5("select 2"), "datasource", "primary").timer());
    }

    @Test
    public void sameBoundary_noCleanup() {
        // n=3，10:00 启动，10:30 和 11:59 都在同一区间 [9:00, 12:00)，不清理
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        clock.setInstant(Instant.parse("2026-08-07T11:59:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);

        // 同区间，两个 Meter 都保留
        assertEquals(2, registry.find("druid.sql.execution.duration").meters().size());
    }

    @Test
    public void exactBoundary_triggersCleanup() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        clock.setInstant(Instant.parse("2026-08-07T12:00:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);

        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5("select 2"),
                        "datasource", "primary").timer());
    }

    // ==================== 跨自然日 ====================

    @Test
    public void crossDay_triggersCleanup() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T23:30:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(6), registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());

        // 跨日到次日 00:05
        clock.setInstant(Instant.parse("2026-08-08T00:05:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);

        // 跨日触发清理，旧 Meter 注销，新 Meter 创建
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());
    }

    // ==================== n 边界 ====================

    @Test
    public void nLe0_cleanupDisabled() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(0), registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        // 跨越很久也不会清理
        clock.setInstant(Instant.parse("2026-08-08T10:00:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);
        assertEquals(2, registry.find("druid.sql.execution.duration").meters().size());
    }

    @Test
    public void nGe24_degradesToDailyCleanup() {
        // n=24 退化为每日一次：同日不清理，跨日才清理
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(24), registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        // 同日 23:00 也不清理（n>=24 同日不清理）
        clock.setInstant(Instant.parse("2026-08-07T23:00:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);
        assertEquals(2, registry.find("druid.sql.execution.duration").meters().size());

        // 跨日触发清理
        clock.setInstant(Instant.parse("2026-08-08T01:00:00Z"));
        listener.onSqlExecute("select 3", dataSource("primary"), 100L, null);
        // 旧的 select 1 / select 2 被清理，select 3 新建
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());
    }

    // ==================== 清理范围 ====================

    @Test
    public void cleanup_clearsSqlAndUriMeters() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        // 创建 SQL 和 URI Meter
        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        listener.onWebRequest(request("/users/{id}", null), "/users/123", 100L, 1, 0, 0, null);
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());
        assertEquals(1, registry.find("druid.uri.request.duration").meters().size());

        // 跨区间触发清理
        clock.setInstant(Instant.parse("2026-08-07T12:05:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);

        // SQL 和 URI Meter 都应被清理（select 2 重新创建）
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());
        assertEquals(0, registry.find("druid.uri.request.duration").meters().size());
    }

    // ==================== 动态刷新 ====================

    @Test
    public void refresh_disableAndReEnablePreservesBaseline() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidStatProperties.Prometheus config = cleanupConfig(3);
        DruidPrometheusMetricsListener listener = newListener(config, registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);

        // 刷新 n=0
        listener.refresh(cleanupConfig(0));

        // 跨 UTC 12:00 也不清理
        clock.setInstant(Instant.parse("2026-08-07T12:05:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);
        assertEquals(2, registry.find("druid.sql.execution.duration").meters().size());

        // 恢复 n=3 后基于原 10:00 基准重算下一个清理点；当前已越过 12:00，下一条事件立即清理。
        listener.refresh(cleanupConfig(3));
        listener.onSqlExecute("select 3", dataSource("primary"), 100L, null);
        assertEquals(1, registry.find("druid.sql.execution.duration").meters().size());
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5("select 3"),
                        "datasource", "primary").timer());
    }

    // ==================== P1-2: 部分 Meter 注销失败 ====================

    @Test
    public void cleanup_partialRemovalFailureStillClearsIdentityAndNewEventRebuilds() {
        // 用一个会针对 Timer 抛 remove 异常的 registry，验证：
        // 1. 清理后 SqlState 始终被移除（即使部分 Meter 注销失败）
        // 2. 新事件重建 SqlState，成功注销的指标新建实例，失败的由 registry 返回旧实例
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        FailingRemoveRegistry registry = new FailingRemoveRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        // 创建 select 1 的 3 个 Meter
        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        String hash1 = DruidPrometheusMetricsListener.calculateSqlMd5("select 1");
        // Timer 存在
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", hash1, "datasource", "primary").timer());
        // affectedRows 存在
        assertNotNull(registry.find("druid.sql.affected.rows")
                .tags("sql", hash1, "datasource", "primary").summary());

        // 让 registry 对 druid.sql.execution.duration 的 remove 抛异常
        registry.failRemoveName("druid.sql.execution.duration");

        // 跨区间触发清理
        clock.setInstant(Instant.parse("2026-08-07T12:05:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);

        // 清理后 select 1 的 affectedRows 应被成功注销（不因 Timer 失败而保留整个 SqlState）
        assertNull(registry.find("druid.sql.affected.rows")
                .tags("sql", hash1, "datasource", "primary").summary());

        // 再次发送 select 1，验证成功注销的 Meter 已重新注册并实际记录
        registry.failRemoveName(null); // 恢复正常 remove
        listener.onSqlExecute("select 1", dataSource("primary"), 500L, null);
        listener.onSqlUpdateCount("select 1", dataSource("primary"), 3);
        // affectedRows 应重新存在并记录了 updateCount=3
        io.micrometer.core.instrument.DistributionSummary affected =
                registry.find("druid.sql.affected.rows")
                        .tags("sql", hash1, "datasource", "primary").summary();
        assertNotNull(affected);
        assertEquals(1L, affected.count());
        assertEquals(3.0, affected.totalAmount(), 0.0);
    }

    // ==================== P1-1: 并发清理/记录不丢数据 ====================

    @Test
    public void concurrent_cleanupAndRecordNoObservationLost() throws Exception {
        // 用可阻塞 Registry 将清理停在 remove() 内，确定性地让另一个事件与清理重叠。
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        BlockingRemoveRegistry registry = new BlockingRemoveRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        listener.onSqlExecute("select 0", dataSource("primary"), 100L, null);
        io.micrometer.core.instrument.Timer timer0Old = registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5("select 0"), "datasource", "primary").timer();
        assertNotNull(timer0Old);
        assertEquals(1L, timer0Old.count());

        clock.setInstant(Instant.parse("2026-08-07T12:05:00Z"));
        final CountDownLatch cleanupDone = new CountDownLatch(1);
        final CountDownLatch recordStarted = new CountDownLatch(1);
        final CountDownLatch recordDone = new CountDownLatch(1);
        final AtomicInteger errors = new AtomicInteger();

        Thread cleanupThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
                } catch (Throwable e) {
                    errors.incrementAndGet();
                } finally {
                    cleanupDone.countDown();
                }
            }
        }, "cleanup-trigger");
        cleanupThread.setDaemon(true);
        cleanupThread.start();

        // 清理线程已经进入 remove()，此时确定持有 cleanupLock 写锁。
        assertTrue(registry.awaitRemoveEntered(5, TimeUnit.SECONDS));
        Thread recordThread = new Thread(new Runnable() {
            @Override public void run() {
                recordStarted.countDown();
                try {
                    listener.onSqlExecute("select 0", dataSource("primary"), 200L, null);
                } catch (Throwable e) {
                    errors.incrementAndGet();
                } finally {
                    recordDone.countDown();
                }
            }
        }, "record-during-cleanup");
        recordThread.setDaemon(true);
        recordThread.start();

        assertTrue(recordStarted.await(5, TimeUnit.SECONDS));
        try {
            // 清理未释放写锁前，记录线程不能拿到已被注销的旧 Meter 并完成记录。
            assertFalse(recordDone.await(200, TimeUnit.MILLISECONDS));
        } finally {
            registry.continueRemoval();
        }

        assertTrue(cleanupDone.await(5, TimeUnit.SECONDS));
        assertTrue(recordDone.await(5, TimeUnit.SECONDS));
        assertEquals(0, errors.get());

        io.micrometer.core.instrument.Timer timer0New = registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5("select 0"), "datasource", "primary").timer();
        io.micrometer.core.instrument.Timer timer1 = registry.find("druid.sql.execution.duration")
                .tags("sql", DruidPrometheusMetricsListener.calculateSqlMd5("select 1"), "datasource", "primary").timer();
        assertNotNull(timer0New);
        assertNotNull(timer1);
        assertFalse(timer0Old == timer0New);
        assertEquals(1L, timer0New.count());
        assertEquals(1L, timer1.count());
    }

    // ==================== P1: 预注册同 ID 外部 Meter 清理后仍存在 ====================

    @Test
    public void cleanup_doesNotRemoveExternallyPreRegisteredMeter() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        // 应用预先注册一个同名同标签的 Timer（外部 Meter）
        String hash1 = DruidPrometheusMetricsListener.calculateSqlMd5("select 1");
        io.micrometer.core.instrument.Tags externalTags =
                io.micrometer.core.instrument.Tags.of("sql", hash1, "datasource", "primary");
        io.micrometer.core.instrument.Timer externalTimer = io.micrometer.core.instrument.Timer
                .builder("druid.sql.execution.duration").tags(externalTags).register(registry);

        // listener 事件触发时会复用这个外部 Timer（registerTimer 检测到已存在不放入 ownedMeters）
        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);

        // 跨区间触发清理
        clock.setInstant(Instant.parse("2026-08-07T12:05:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 100L, null);

        // 外部预注册的 Timer 不应被清理删除
        io.micrometer.core.instrument.Timer timerAfter = registry.find("druid.sql.execution.duration")
                .tags("sql", hash1, "datasource", "primary").timer();
        assertNotNull(timerAfter);
        assertSame(externalTimer, timerAfter);
    }

    // ==================== 触发事件清理后正常注册 ====================

    @Test
    public void triggeringEventRegistersNewMeterAfterCleanup() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-07T10:00:00Z"), ZoneOffset.UTC);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        DruidPrometheusMetricsListener listener = newListener(cleanupConfig(3), registry, clock);
        listener.init();

        listener.onSqlExecute("select 1", dataSource("primary"), 100L, null);
        String hash1 = DruidPrometheusMetricsListener.calculateSqlMd5("select 1");

        // 跨区间，select 2 触发清理
        clock.setInstant(Instant.parse("2026-08-07T12:05:00Z"));
        listener.onSqlExecute("select 2", dataSource("primary"), 200L, null);
        String hash2 = DruidPrometheusMetricsListener.calculateSqlMd5("select 2");

        // select 2 的新 Meter 应已记录
        assertNotNull(registry.find("druid.sql.execution.duration")
                .tags("sql", hash2, "datasource", "primary").timer());
        // select 1 的旧 Meter 应已注销
        assertNull(registry.find("druid.sql.execution.duration")
                .tags("sql", hash1, "datasource", "primary").timer());
    }

    // ==================== 工具方法 ====================

    private DruidPrometheusMetricsListener newListener(DruidStatProperties.Prometheus config,
                                                        MeterRegistry registry,
                                                        Clock clock) {
        DruidPrometheusMetricsListener listener = new DruidPrometheusMetricsListener(
                config, provider(registry), provider(null), clock);
        listeners.add(listener);
        return listener;
    }

    private static DruidStatProperties.Prometheus cleanupConfig(int intervalHours) {
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getSqlMapping().setEnabled(false);
        config.getEvents().getCleanup().setIntervalHours(intervalHours);
        return config;
    }

    private static DataSourceProxy dataSource(String name) {
        DataSourceProxy ds = mock(DataSourceProxy.class);
        when(ds.getName()).thenReturn(name);
        return ds;
    }

    private static HttpServletRequest request(String mvcPattern, String contextPath) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getAttribute("org.springframework.web.servlet.HandlerMapping.bestMatchingPattern"))
                .thenReturn(mvcPattern);
        when(request.getContextPath()).thenReturn(contextPath);
        return request;
    }

    private static <T> ObjectProvider<T> provider(final T value) {
        return new ObjectProvider<T>() {
            @Override public T getObject(Object... args) throws BeansException { return value; }
            @Override public T getIfAvailable() throws BeansException { return value; }
            @Override public T getIfUnique() throws BeansException { return value; }
            @Override public T getObject() throws BeansException { return value; }
        };
    }

    /** 可变时钟，测试用 setInstant/advance 推进时间。 */
    private static final class MutableClock extends Clock {
        private volatile Instant instant;
        private final ZoneId zone;

        MutableClock(Instant instant, ZoneId zone) {
            this.instant = instant;
            this.zone = zone;
        }

        @Override public ZoneId getZone() { return zone; }
        @Override public Clock withZone(ZoneId zone) { return new MutableClock(instant, zone); }
        @Override public Instant instant() { return instant; }

        void setInstant(Instant instant) { this.instant = instant; }

        void advance(long amount, TimeUnit unit) {
            this.instant = instant.plusNanos(unit.toNanos(amount));
        }
    }

    /**
     * 可配置对指定名称的 Meter 的 remove 抛异常，用于验证部分注销失败场景。
     */
    private static final class FailingRemoveRegistry extends SimpleMeterRegistry {
        private volatile String failName;

        void failRemoveName(String name) { this.failName = name; }

        @Override
        public Meter remove(Meter meter) {
            if (failName != null && meter != null && failName.equals(meter.getId().getName())) {
                throw new RuntimeException("simulated remove failure for " + failName);
            }
            return super.remove(meter);
        }
    }

    /** 第一次 remove 时阻塞，用于构造清理持写锁且业务事件同时进入的确定性时序。 */
    private static final class BlockingRemoveRegistry extends SimpleMeterRegistry {
        private final CountDownLatch removeEntered = new CountDownLatch(1);
        private final CountDownLatch continueRemoval = new CountDownLatch(1);
        private final AtomicBoolean blocked = new AtomicBoolean();

        boolean awaitRemoveEntered(long timeout, TimeUnit unit) throws InterruptedException {
            return removeEntered.await(timeout, unit);
        }

        void continueRemoval() {
            continueRemoval.countDown();
        }

        @Override
        public Meter remove(Meter meter) {
            if (blocked.compareAndSet(false, true)) {
                removeEntered.countDown();
                try {
                    if (!continueRemoval.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("timed out waiting to continue meter removal");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while waiting to continue meter removal", e);
                }
            }
            return super.remove(meter);
        }
    }
}
