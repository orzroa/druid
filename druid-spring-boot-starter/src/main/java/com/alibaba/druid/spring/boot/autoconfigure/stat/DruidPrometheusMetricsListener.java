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

import com.alibaba.druid.filter.stat.StatFilterContext;
import com.alibaba.druid.filter.stat.StatFilterEventListener;
import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import com.alibaba.druid.support.http.WebStatEventContext;
import com.alibaba.druid.support.http.WebStatEventListener;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * 将 Druid 原生的统计事件（SQL 执行、Web 请求）记录到应用已有的 {@link MeterRegistry} 中，
 * 从而通过 Micrometer 暴露给 Prometheus（由业务的 {@code /actuator/prometheus} 端点采集）。
 *
 * <p>核心职责：
 * <ul>
 *     <li>作为 Druid 的 {@link StatFilterEventListener} / {@link WebStatEventListener}，
 *         订阅 SQL 与 Web 事件；</li>
 *     <li>实现 {@link DruidPrometheusMetricsRefresher}，支持在运行时替换配置快照；</li>
 *     <li>实现 {@link BeanFactoryAware}，用于在运行时反查 DataSource 对应的 Spring Bean 名称。</li>
 * </ul>
 *
 * <p>设计要点：
 * <ul>
 *     <li>不自行启动周期性刷新线程，也不再通过旧的 JSON 端点暴露指标；</li>
 *     <li>使用唯一的单写线程（{@link #mappingExecutor}）把 SQL 文本按 MD5 落盘，
 *         避免重复提交与阻塞业务事件路径；</li>
 *     <li>对 SQL / URI 的标签基数（-identity 上限）使用近似 LRU 缓存，淘汰最久未使用的 Meter，防止 Meter 爆炸。</li>
 * </ul>
 */
public final class DruidPrometheusMetricsListener implements StatFilterEventListener, WebStatEventListener,
        DruidPrometheusMetricsRefresher, BeanFactoryAware {
    /** 明细指标排障日志；仅在 TRACE 级别下输出身份、Meter 和清理细节。 */
    private static final Logger DETAIL_LOG = LoggerFactory.getLogger("druid.prometheus.detail");
    /** Spring MVC 在请求属性中存放“最佳匹配路径模板”的 key，例如 {@code /users/{id}}。 */
    private static final String SPRING_MVC_PATTERN_ATTRIBUTE =
            "org.springframework.web.servlet.HandlerMapping.bestMatchingPattern";
    private static final Duration DEFAULT_MAX_WINDOW = Duration.ofMinutes(2);
    private static final String SQL_EXECUTION_DURATION = "druid.sql.execution.duration";
    private static final String SQL_AFFECTED_ROWS = "druid.sql.affected.rows";
    private static final String SQL_FETCHED_ROWS = "druid.sql.fetched.rows";
    private static final String URI_REQUEST_DURATION = "druid.uri.request.duration";
    private static final String URI_JDBC_EXECUTIONS = "druid.uri.jdbc.executions";
    private static final String URI_JDBC_AFFECTED_ROWS = "druid.uri.jdbc.affected.rows";
    private static final String URI_JDBC_FETCHED_ROWS = "druid.uri.jdbc.fetched.rows";

    /** 当前的 Prometheus 配置快照（运行时可被 {@link #refresh} 原子替换）。 */
    private volatile DruidStatProperties.Prometheus config;
    /** 应用已有的 MeterRegistry 提供者（Spring 注入，延迟到 init 时取出）。 */
    private final ObjectProvider<MeterRegistry> meterRegistryProvider;
    /** 可选的 URI 模板解析器（SPI），用于把原始 URI 解析为模板以压缩基数。 */
    private final ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider;

    /**
     * 以 {@code (sqlMd5 + '\u0000' + dataSourceName)} 为 key 缓存的 SQL 状态。
     * 首次见到的 SQL 会创建 Meter，之后再出现则复用已有 Meter。
     */
    private final ConcurrentMap<String, SqlState> sqlStates = new ConcurrentHashMap<String, SqlState>();
    /** 以 URI 模板为 key 缓存的 URI 指标 Meter。 */
    private final ConcurrentMap<String, UriMeters> uriMeters = new ConcurrentHashMap<String, UriMeters>();
    /** 注销失败的 Meter 句柄，下个清理周期重试。与 identity/LRU 解耦，避免拖累正常事件路径。 */
    private final ConcurrentMap<Meter.Id, Meter> failedRemovals = new ConcurrentHashMap<Meter.Id, Meter>();
    /** 仅序列注册/淘汰时使用，正常命中路径不获取此锁。 */
    private final Object sqlMeterLock = new Object();
    /** 仅序列注册/淘汰时使用，正常命中路径不获取此锁。 */
    private final Object uriMeterLock = new Object();
    /** 记录 DataSourceProxy 实例到其 Spring Bean 名称的映射，避免反复扫描容器。 */
    private final ConcurrentMap<DataSourceProxy, String> dataSourceBeanNames =
            new ConcurrentHashMap<DataSourceProxy, String>();

    /** 实际取出并缓存的 MeterRegistry；为 null 时表示未启用指标，所有事件都会被跳过。 */
    private volatile MeterRegistry meterRegistry;
    /** Micrometer 1.1.x Prometheus 暴露层清理适配器。 */
    private volatile DruidPrometheusCollectorCleanup collectorCleanup;
    /** 单线程、有界队列的 SQL 映射落盘执行器。 */
    private volatile ThreadPoolExecutor mappingExecutor;
    /** 容器工厂（用于反查 DataSource Bean 名）；非 ListableBeanFactory 时为 null。 */
    private volatile ListableBeanFactory beanFactory;

    /** 1.5 期高基数明细 Meter 清理调度、并发控制与日志组件。 */
    private final DruidPrometheusMeterCleanup meterCleanup;

    /**
     * 构造器。由 Spring 自动注入配置与两个 Provider。
     *
     * @param config                      初始 Prometheus 配置快照
     * @param meterRegistryProvider       应用 MeterRegistry 提供者
     * @param uriTemplateResolverProvider URI 模板解析器提供者（可选）
     */
    public DruidPrometheusMetricsListener(DruidStatProperties.Prometheus config,
                                           ObjectProvider<MeterRegistry> meterRegistryProvider,
                                           ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider) {
        this(config, meterRegistryProvider, uriTemplateResolverProvider, Clock.systemDefaultZone());
    }

    /**
     * 测试专用构造器，可注入 {@link Clock} 以验证时间边界场景。生产代码使用
     * {@link #DruidPrometheusMetricsListener(DruidStatProperties.Prometheus, ObjectProvider, ObjectProvider)}。
     *
     * @param clock                       时间源
     */
    DruidPrometheusMetricsListener(DruidStatProperties.Prometheus config,
                                    ObjectProvider<MeterRegistry> meterRegistryProvider,
                                    ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider,
                                    Clock clock) {
        this.config = config;
        this.meterRegistryProvider = meterRegistryProvider;
        this.uriTemplateResolverProvider = uriTemplateResolverProvider;
        this.meterCleanup = new DruidPrometheusMeterCleanup(clock, new DruidPrometheusMeterCleanup.Handler() {
            @Override
            public DruidPrometheusMeterCleanup.CleanupCounts cleanup() {
                return cleanupDetailMeters();
            }

            @Override
            public String dataSourceName(DataSourceProxy dataSource) {
                return DruidPrometheusMetricsListener.this.dataSourceName(dataSource);
            }
        });
    }

    /**
     * 初始化：取出 MeterRegistry，并在可用时创建落盘线程和事件监听注册。
     * 若容器中没有 MeterRegistry，则直接返回（指标功能视为关闭）。
     */
    @PostConstruct
    public void init() {
        meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry == null) {
            return;
        }
        collectorCleanup = new DruidPrometheusCollectorCleanup(meterRegistry);
        collectorCleanup.logBinding("init");
        meterCleanup.updateInterval(config.getEvents().getCleanup().getIntervalHours());
        int queueSize = Math.max(1, config.getSqlMapping().getQueueSize());
        // 单线程 + 有界队列 + 拒绝即抛 AbortPolicy，保证落盘不会无限堆积、也不会阻塞事件线程
        mappingExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueSize), new MappingThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        // 向 Druid 注册自己，开始接收 SQL / Web 事件
        StatFilterContext.getInstance().addEventListener(this);
        WebStatEventContext.addListener(this);
    }

    /**
     * 销毁：注销事件监听并立即关闭落盘线程，避免泄漏。
     */
    @PreDestroy
    public void destroy() {
        StatFilterContext.getInstance().removeEventListener(this);
        WebStatEventContext.removeListener(this);
        ThreadPoolExecutor executor = mappingExecutor;
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /**
     * {@link DruidPrometheusMetricsRefresher} 的实现：在运行时原子替换配置快照。
     * 只替换非 null 的参数；对后续事件立即生效。identity 上限缩小时，下一次访问
     * 对应类型的事件会按近似 LRU 淘汰多余 Meter。
     *
     * @param config 新的 Prometheus 配置快照
     */
    @Override
    public void refresh(DruidStatProperties.Prometheus config) {
        if (config != null) {
            Lock writeLock = meterCleanup.writeLock();
            writeLock.lock();
            try {
                this.config = config;
                // 配置刷新是低频操作；与清理串行更新周期和下次截止时间，避免混用新旧配置。
                meterCleanup.updateIntervalWhileLocked(config.getEvents().getCleanup().getIntervalHours());
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * 缓存 Spring 容器工厂，便于后续根据 DataSource 实例反查其 Bean 名称。
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ListableBeanFactory) {
            this.beanFactory = (ListableBeanFactory) beanFactory;
        }
    }

    /**
     * SQL 执行完成事件。仅记录成功（error == null）且非空 SQL 的耗时。
     */
    @Override
    public void onSqlExecute(String sql, DataSourceProxy dataSource, long durationNanos, Throwable error) {
        if (!isEnabled() || error != null || sql == null || sql.length() == 0) {
            return;
        }
        meterCleanup.maybeCleanupSql(dataSource);
        Lock readLock = meterCleanup.readLock();
        readLock.lock();
        try {
            SqlState state = sqlState(sql, dataSource);
            if (state != null && state.meters != null) {
                state.meters.timer.record(durationNanos, TimeUnit.NANOSECONDS);
                traceRecord(state.meters.timer, "sql-execute", durationNanos, "nanoseconds");
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * SQL 更新条数事件（如 insert/update/delete 的 affected rows）。
     * updateCount < 0 表示未知，不记录。
     */
    @Override
    public void onSqlUpdateCount(String sql, DataSourceProxy dataSource, int updateCount) {
        if (!isEnabled() || sql == null || sql.length() == 0) {
            return;
        }
        meterCleanup.maybeCleanupSql(dataSource);
        Lock readLock = meterCleanup.readLock();
        readLock.lock();
        try {
            SqlState state = sqlState(sql, dataSource);
            if (updateCount >= 0 && state != null && state.meters != null) {
                state.meters.affectedRows.record(updateCount);
                traceRecord(state.meters.affectedRows, "sql-update-count", updateCount, "rows");
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * ResultSet 关闭事件：记录该次查询抓取的行数（fetched rows）。
     * 仅在 ResultSet 关闭时才记录，符合“查询完成后才统计”的语义。
     */
    @Override
    public void onSqlResultSetClose(String sql, DataSourceProxy dataSource, int fetchRowCount) {
        if (!isEnabled() || sql == null || sql.length() == 0) {
            return;
        }
        meterCleanup.maybeCleanupSql(dataSource);
        Lock readLock = meterCleanup.readLock();
        readLock.lock();
        try {
            SqlState state = sqlState(sql, dataSource);
            if (state != null && state.meters != null) {
                state.meters.fetchedRows.record(fetchRowCount);
                traceRecord(state.meters.fetchedRows, "sql-resultset-close", fetchRowCount, "rows");
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Web 请求事件：解析 URI 模板，记录请求耗时、以及本次请求触发的 JDBC 执行/影响/抓取行数。
     */
    @Override
    public void onWebRequest(HttpServletRequest request, String uri, long durationNanos,
                             long jdbcExecuteCount, long jdbcUpdateCount,
                             long jdbcFetchRowCount, Throwable error) {
        if (!isEnabled()) {
            return;
        }
        // 解析为模板（应用 SPI → Spring MVC pattern → 原始 URI），以压缩 URI 标签基数
        String template = resolveUri(request, uri);
        if (template == null || template.length() == 0) {
            return;
        }
        meterCleanup.maybeCleanupUri(template);
        Lock readLock = meterCleanup.readLock();
        readLock.lock();
        try {
            UriMeters meters = uriMeters(template);
            if (meters == null) {
                return;
            }
            meters.timer.record(durationNanos, TimeUnit.NANOSECONDS);
            traceRecord(meters.timer, "web-request", durationNanos, "nanoseconds");
            if (jdbcExecuteCount >= 0) {
                meters.jdbcExecutions.record(jdbcExecuteCount);
                traceRecord(meters.jdbcExecutions, "web-jdbc-executions", jdbcExecuteCount, "count");
            }
            if (jdbcUpdateCount >= 0) {
                meters.jdbcAffectedRows.record(jdbcUpdateCount);
                traceRecord(meters.jdbcAffectedRows, "web-jdbc-affected-rows", jdbcUpdateCount, "rows");
            }
            if (jdbcFetchRowCount >= 0) {
                meters.jdbcFetchedRows.record(jdbcFetchRowCount);
                traceRecord(meters.jdbcFetchedRows, "web-jdbc-fetched-rows", jdbcFetchRowCount, "rows");
            }
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 判断指标收集是否在当前配置下启用：必须有 MeterRegistry、且 enabled 与 events.enabled 均为 true。
     */
    private boolean isEnabled() {
        return meterRegistry != null && config.isEnabled() && config.getEvents().isEnabled();
    }

    private void traceRecord(Meter meter, String source, long value, String unit) {
        // 逐条事件记录值只用于短时排障，避免 INFO 日志干扰正常运行。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail meter update: action=record source={} meter={} value={} unit={}",
                    source, meter.getId(), value, unit);
        }
    }

    /**
     * 执行实际的清理：注销 Registry 中所有一期 SQL/URI 明细 Meter，清空 identity/LRU/句柄。
     * 调用方 {@link DruidPrometheusMeterCleanup} 持有写锁。
     * 始终移除 identity/LRU 状态；注销失败的单个 Meter 句柄存入 {@link #failedRemovals}，下周期重试。
     * 不能只依赖本 Listener 的 identity 缓存：同名明细 Meter 可能在本 Listener 建立缓存前
     * 就已存在，或者此前本地 identity 已淘汰；这两种情况都必须在周期清理时回收。
     */
    private DruidPrometheusMeterCleanup.CleanupCounts cleanupDetailMeters() {
        // 清理开始前记录本地缓存、重试队列和 Registry 的规模。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail meter cleanup: action=start registryMeters={} sqlStates={} uriStates={} pendingRetries={}",
                    meterRegistry.getMeters().size(), sqlStates.size(), uriMeters.size(), failedRemovals.size());
        }
        // 先重试上一周期注销失败的 Meter
        int retrySuccess = 0, retryFailed = 0;
        if (!failedRemovals.isEmpty()) {
            Iterator<Map.Entry<Meter.Id, Meter>> retryIt = failedRemovals.entrySet().iterator();
            while (retryIt.hasNext()) {
                Map.Entry<Meter.Id, Meter> entry = retryIt.next();
                try {
                    meterRegistry.remove(entry.getValue());
                    if (!removePrometheusCollector(entry.getValue())) {
                        throw new IllegalStateException("Prometheus collector cleanup incomplete for " + entry.getKey());
                    }
                    retryIt.remove();
                    retrySuccess++;
                    // 记录上周期失败 Meter 的重试成功结果。
                    if (DETAIL_LOG.isTraceEnabled()) {
                        DETAIL_LOG.trace("druid detail meter delete: action=retry-remove result=success meter={}",
                                entry.getKey());
                    }
                } catch (RuntimeException e) {
                    retryFailed++;
                    // 记录重试失败原因；句柄会保留到下一个周期继续尝试。
                    if (DETAIL_LOG.isTraceEnabled()) {
                        DETAIL_LOG.trace("druid detail meter delete: action=retry-remove result=failed meter={} error={}",
                                entry.getKey(), e.toString());
                    }
                    // 保留在 failedRemovals 供下次重试
                }
            }
        }

        // 重试完成后再取 Registry 快照，避免同一个 Meter 被成功重试后又重复计数。
        Map<Meter.Id, Meter> sqlMeters = new LinkedHashMap<Meter.Id, Meter>();
        Map<Meter.Id, Meter> uriDetailMeters = new LinkedHashMap<Meter.Id, Meter>();
        collectDetailMeters(sqlMeters, uriDetailMeters);
        int sqlBefore = countSqlIdentities(sqlMeters);
        int uriBefore = countUriIdentities(uriDetailMeters);

        // 1. 注销 SQL Meter；扫描 Registry，不能遗漏不在本地 identity 缓存中的孤儿 Meter。
        int sqlSuccess = 0, sqlFailed = 0;
        for (Meter meter : sqlMeters.values()) {
            if (removeMeterOrDefer(meter, "periodic-cleanup")) sqlSuccess++; else sqlFailed++;
        }
        sqlStates.clear();

        // 2. 注销 URI Meter；同样扫描 Registry。
        int uriSuccess = 0, uriFailed = 0;
        for (Meter meter : uriDetailMeters.values()) {
            if (removeMeterOrDefer(meter, "periodic-cleanup")) uriSuccess++; else uriFailed++;
        }
        uriMeters.clear();

        DruidPrometheusMeterCleanup.CleanupCounts counts = new DruidPrometheusMeterCleanup.CleanupCounts(sqlBefore, uriBefore,
                sqlSuccess, sqlFailed, uriSuccess, uriFailed, retrySuccess, retryFailed);
        DruidPrometheusCollectorCleanup cleanup = collectorCleanup;
        if (cleanup != null) {
            cleanup.logBinding("cleanup");
        }
        // 汇总本周期 SQL、URI 和失败重试的注销结果。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail meter cleanup: action=finish sqlBefore={} uriBefore={} sqlSuccess={} "
                            + "sqlFailed={} uriSuccess={} uriFailed={} retrySuccess={} retryFailed={} registryMeters={}",
                    sqlBefore, uriBefore, sqlSuccess, sqlFailed, uriSuccess, uriFailed,
                    retrySuccess, retryFailed, meterRegistry.getMeters().size());
        }
        return counts;
    }

    /** 收集 Registry 中本模块全部高基数 SQL/URI 明细 Meter，按 ID 去重。 */
    private void collectDetailMeters(Map<Meter.Id, Meter> sqlMeters, Map<Meter.Id, Meter> uriDetailMeters) {
        for (Meter meter : meterRegistry.getMeters()) {
            String name = meter.getId().getName();
            if (isSqlDetailMeter(name)) {
                sqlMeters.put(meter.getId(), meter);
                // 输出扫描到的 SQL 明细 Meter，辅助排查孤儿序列。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail meter query: action=cleanup-scan type=sql meter={}", meter.getId());
                }
            } else if (isUriDetailMeter(name)) {
                uriDetailMeters.put(meter.getId(), meter);
                // 输出扫描到的 URI 明细 Meter，辅助排查孤儿序列。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail meter query: action=cleanup-scan type=uri meter={}", meter.getId());
                }
            }
        }
    }

    private static boolean isSqlDetailMeter(String name) {
        return SQL_EXECUTION_DURATION.equals(name)
                || SQL_AFFECTED_ROWS.equals(name)
                || SQL_FETCHED_ROWS.equals(name);
    }

    private static boolean isUriDetailMeter(String name) {
        return URI_REQUEST_DURATION.equals(name)
                || URI_JDBC_EXECUTIONS.equals(name)
                || URI_JDBC_AFFECTED_ROWS.equals(name)
                || URI_JDBC_FETCHED_ROWS.equals(name);
    }

    private static boolean isDetailMeter(String name) {
        return isSqlDetailMeter(name) || isUriDetailMeter(name);
    }

    private static int countSqlIdentities(Map<Meter.Id, Meter> meters) {
        Map<String, Boolean> identities = new LinkedHashMap<String, Boolean>();
        for (Meter.Id id : meters.keySet()) {
            identities.put(id.getTag("sql") + '\u0000' + id.getTag("datasource"), Boolean.TRUE);
        }
        return identities.size();
    }

    private static int countUriIdentities(Map<Meter.Id, Meter> meters) {
        Map<String, Boolean> identities = new LinkedHashMap<String, Boolean>();
        for (Meter.Id id : meters.keySet()) {
            identities.put(id.getTag("uri"), Boolean.TRUE);
        }
        return identities.size();
    }

    /**
     * 注销单个 Meter；成功返回 true。失败时把句柄存入 {@link #failedRemovals} 供下周期重试，返回 false。
     * SQL/URI 明细 Meter 的命名空间由本模块管理，直接调用 registry.remove；注销失败时
     * 保留句柄重试，避免留下无法发现的孤儿序列。
     */
    private boolean removeMeterOrDefer(Meter meter, String source) {
        if (meter == null) return true;
        if (!isDetailMeter(meter.getId().getName())) {
            // 记录非本模块命名空间的 Meter 被保护性跳过。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail meter delete: action=remove result=skipped-not-detail source={} meter={}",
                        source, meter.getId());
            }
            return true;
        }
        try {
            meterRegistry.remove(meter);
            if (!removePrometheusCollector(meter)) {
                throw new IllegalStateException("Prometheus collector cleanup incomplete for " + meter.getId());
            }
            failedRemovals.remove(meter.getId(), meter);
            // 记录单个 Meter 的正常注销来源和结果。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail meter delete: action=remove result=success source={} meter={}",
                        source, meter.getId());
            }
            return true;
        } catch (RuntimeException e) {
            failedRemovals.put(meter.getId(), meter);
            meterCleanup.warnRemovalFailure(meter.getId(), e);
            // 记录注销失败原因；后续周期将从失败队列重试。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail meter delete: action=remove result=failed source={} meter={} error={}",
                        source, meter.getId(), e.toString());
            }
            return false;
        }
    }

    /** 同步清理 Prometheus 暴露层；非 Prometheus Registry 无需额外处理。 */
    private boolean removePrometheusCollector(Meter meter) {
        DruidPrometheusCollectorCleanup cleanup = collectorCleanup;
        return cleanup == null || cleanup.remove(meter);
    }

    /**
     * 取得（必要时懒创建）某个 SQL 的 {@link SqlState}。
     * 内部维护近似 LRU 缓存。正常命中仅更新时间戳，不获取互斥锁；超过
     * {@code max-sql-identities} 上限时，
     * 淘汰最久未使用的 Meter，并为当前 SQL 创建新的 Meter。
     *
     * @return SQL 状态；无法处理时返回 null
     */
    private SqlState sqlState(String sql, DataSourceProxy dataSource) {
        String hash = calculateSqlMd5(sql);
        if (hash.length() == 0) {
            return null;
        }
        String dataSourceName = dataSourceName(dataSource);
        String key = hash + '\u0000' + dataSourceName;
        SqlState current = sqlStates.get(key);
        if (current != null) {
            // 记录 SQL identity 的无锁命中，便于分析缓存命中率。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail identity query: type=sql result=hit sql={} datasource={}",
                        hash, dataSourceName);
            }
            current.touch();
            trimSqlStatesIfNecessary();
            return current;
        }
        // 记录 SQL identity 的首次未命中，后续将尝试创建对应 Meter。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail identity query: type=sql result=miss sql={} datasource={}",
                    hash, dataSourceName);
        }
        // 调用方已持有 meterCleanup 读锁，防止与清理写锁并发
        synchronized (sqlMeterLock) {
            current = sqlStates.get(key);
            if (current != null) {
                // 记录等待建表锁期间被其他线程创建完成的命中。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail identity query: type=sql result=hit-after-lock sql={} datasource={}",
                            hash, dataSourceName);
                }
                current.touch();
                trimSqlStates(maxSqlIdentities());
                return current;
            }
            SqlState candidate = new SqlState(hash, dataSourceName, sql);
            try {
                candidate.meters = createSqlMeters(candidate.hash, candidate.dataSource);
            } catch (RuntimeException e) {
                // 记录 SQL Meter 注册失败，事件本身仍会被安全跳过。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail identity add: type=sql result=failed sql={} datasource={} error={}",
                            hash, dataSourceName, e.toString());
                }
                return null;
            }
            trimSqlStates(maxSqlIdentities() - 1);
            sqlStates.put(key, candidate);
            // 记录 SQL identity 和整组 Meter 已成功加入缓存。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail identity add: type=sql result=success sql={} datasource={} size={}",
                        hash, dataSourceName, sqlStates.size());
            }
            submitMapping(candidate);
            return candidate;
        }
    }

    /** 获取 URI 的近似 LRU Meter；新 URI 会淘汰最久未使用的 Meter。 */
    private UriMeters uriMeters(String template) {
        UriMeters meters = uriMeters.get(template);
        if (meters != null) {
            // 记录 URI identity 的无锁命中，便于分析缓存命中率。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail identity query: type=uri result=hit uri={}", template);
            }
            meters.touch();
            trimUriMetersIfNecessary();
            return meters;
        }
        // 记录 URI identity 的首次未命中，后续将尝试创建对应 Meter。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail identity query: type=uri result=miss uri={}", template);
        }
        // 调用方已持有 meterCleanup 读锁，防止与清理写锁并发
        synchronized (uriMeterLock) {
            meters = uriMeters.get(template);
            if (meters != null) {
                // 记录等待建表锁期间被其他线程创建完成的命中。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail identity query: type=uri result=hit-after-lock uri={}", template);
                }
                meters.touch();
                trimUriMeters(maxUriIdentities());
                return meters;
            }
            try {
                meters = createUriMeters(template);
            } catch (RuntimeException e) {
                // 记录 URI Meter 注册失败，事件本身仍会被安全跳过。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail identity add: type=uri result=failed uri={} error={}",
                            template, e.toString());
                }
                return null;
            }
            trimUriMeters(maxUriIdentities() - 1);
            uriMeters.put(template, meters);
            // 记录 URI identity 和整组 Meter 已成功加入缓存。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail identity add: type=uri result=success uri={} size={}",
                        template, uriMeters.size());
            }
            return meters;
        }
    }

    /**
     * 把“写 SQL 映射文件”的任务提交给落盘线程（Meter 已在 {@link #sqlState} 中同步创建）。
     * 若线程不可用或队列已满，仅跳过落盘，不丢弃已创建的 Meter，避免丢失观测。
     */
    private void submitMapping(final SqlState state) {
        ThreadPoolExecutor executor = mappingExecutor;
        if (executor == null) {
            // 无落盘线程：保留 Meter，仅释放 SQL 原文
            state.sql = null;
            return;
        }
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        writeSqlMapping(state.hash, state.sql);
                    } catch (RuntimeException ignored) {
                        // 落盘失败不影响已有 Meter，仅放弃本次映射写盘
                    } finally {
                        // SQL 文本已落盘（或放弃落盘），释放内存中的原文
                        state.sql = null;
                    }
                }
            });
        } catch (RuntimeException e) {
            // 队列满触发 AbortPolicy：保留 Meter，仅释放 SQL 原文
            state.sql = null;
        }
    }

    /**
     * 为某条 SQL 创建三个 Meter：
     * <ul>
     *     <li>{@code druid.sql.execution.duration} — 执行耗时（Timer，直方图按滑动窗口分布）；</li>
     *     <li>{@code druid.sql.affected.rows} — 影响行数（DistributionSummary）；</li>
     *     <li>{@code druid.sql.fetched.rows} — 抓取行数（DistributionSummary）。</li>
     * </ul>
     * 标签为 {@code sql=md5, datasource=名称}；分布统计半衰期取 max-window 的一半、buffer=2。
     */
    private SqlMeters createSqlMeters(String hash, String dataSource) {
        Tags tags = Tags.of("sql", hash, "datasource", dataSource);
        Duration expiry = maxWindow().dividedBy(2);
        Timer timer = null;
        DistributionSummary affected = null;
        DistributionSummary fetched = null;
        try {
            timer = registerDetailTimer(Timer.builder(SQL_EXECUTION_DURATION)
                    .tags(tags).distributionStatisticExpiry(expiry)
                    .distributionStatisticBufferLength(2), SQL_EXECUTION_DURATION, tags);
            affected = registerDetailSummary(DistributionSummary.builder(SQL_AFFECTED_ROWS)
                    .tags(tags).distributionStatisticExpiry(expiry)
                    .distributionStatisticBufferLength(2), SQL_AFFECTED_ROWS, tags);
            fetched = registerDetailSummary(DistributionSummary.builder(SQL_FETCHED_ROWS)
                    .tags(tags).distributionStatisticExpiry(expiry)
                    .distributionStatisticBufferLength(2), SQL_FETCHED_ROWS, tags);
            return new SqlMeters(timer, affected, fetched);
        } catch (RuntimeException e) {
            removeMeterOrDefer(timer, "sql-registration-rollback");
            removeMeterOrDefer(affected, "sql-registration-rollback");
            removeMeterOrDefer(fetched, "sql-registration-rollback");
            throw e;
        }
    }

    /**
     * 为某个 URI 创建四个 Meter（标签 {@code uri=模板}）：
     * <ul>
     *     <li>{@code druid.uri.request.duration} — 请求耗时（Timer）；</li>
     *     <li>{@code druid.uri.jdbc.executions} — 触发的 JDBC 执行次数；</li>
     *     <li>{@code druid.uri.jdbc.affected.rows} — 影响行数；</li>
     *     <li>{@code druid.uri.jdbc.fetched.rows} — 抓取行数。</li>
     * </ul>
     * 上限由调用方的近似 LRU 缓存控制。
     */
    private UriMeters createUriMeters(String uri) {
        Tags tags = Tags.of("uri", uri);
        Duration expiry = maxWindow().dividedBy(2);
        Timer timer = null;
        DistributionSummary executions = null;
        DistributionSummary affectedRows = null;
        DistributionSummary fetchedRows = null;
        try {
            timer = registerDetailTimer(Timer.builder(URI_REQUEST_DURATION).tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    URI_REQUEST_DURATION, tags);
            executions = registerDetailSummary(DistributionSummary.builder(URI_JDBC_EXECUTIONS).tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    URI_JDBC_EXECUTIONS, tags);
            affectedRows = registerDetailSummary(DistributionSummary.builder(URI_JDBC_AFFECTED_ROWS).tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    URI_JDBC_AFFECTED_ROWS, tags);
            fetchedRows = registerDetailSummary(DistributionSummary.builder(URI_JDBC_FETCHED_ROWS).tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    URI_JDBC_FETCHED_ROWS, tags);
            return new UriMeters(timer, executions, affectedRows, fetchedRows);
        } catch (RuntimeException e) {
            removeMeterOrDefer(timer, "uri-registration-rollback");
            removeMeterOrDefer(executions, "uri-registration-rollback");
            removeMeterOrDefer(affectedRows, "uri-registration-rollback");
            removeMeterOrDefer(fetchedRows, "uri-registration-rollback");
            throw e;
        }
    }

    /**
     * 将 SQL 文本按 MD5 文件名落盘，便于后续在 Grafana 等侧把 md5 还原为可读 SQL。
     * 使用原子移动避免并发重复写；已存在同名文件则跳过。
     */
    private void writeSqlMapping(String hash, String sql) {
        if (!config.getSqlMapping().isEnabled()) {
            return;
        }
        Path directory = Paths.get(config.getSqlMapping().getDirectory());
        // 落盘文件名带上 .sql 后缀，便于人工识别与编辑器语法高亮
        Path target = directory.resolve(hash + ".sql");
        try {
            if (Files.exists(target)) {
                return;
            }
            Files.createDirectories(directory);
            if (Files.exists(target)) {
                return;
            }
            // 先写临时文件再原子重命名，保证目标文件要么完整存在、要么不存在
            Path temporary = Files.createTempFile(directory, hash, ".sql.tmp");
            try {
                Files.write(temporary, sql.getBytes(StandardCharsets.UTF_8));
                try {
                    try {
                        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                    } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
                        Files.move(temporary, target);
                    }
                } catch (java.nio.file.FileAlreadyExistsException ignored) {
                    // 其他进程/线程已先创建了相同的 MD5 映射，忽略
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (IOException e) {
            throw new IllegalStateException("write SQL mapping failed", e);
        }
    }

    /**
     * 解析 URI 模板，优先级为：
     * <ol>
     *     <li>应用提供的 {@link DruidUriTemplateResolver#resolve}（SPI）；</li>
     *     <li>Spring MVC 的 best-matching pattern 属性；</li>
     *     <li>原始 URI 作为兜底。</li>
     * </ol>
     * 若开启了 {@code include-context-path}，则拼接 contextPath 前缀。
     */
    private String resolveUri(HttpServletRequest request, String fallback) {
        DruidUriTemplateResolver resolver = uriTemplateResolverProvider.getIfAvailable();
        String value = resolver == null ? null : resolver.resolve(request);
        if (value == null || value.length() == 0) {
            Object pattern = request.getAttribute(SPRING_MVC_PATTERN_ATTRIBUTE);
            value = pattern instanceof String ? (String) pattern : null;
        }
        if (value == null || value.length() == 0) {
            value = fallback;
        }
        if (config.getUriTemplate().isIncludeContextPath() && value != null
                && value.startsWith("/") && request.getContextPath() != null) {
            value = request.getContextPath() + value;
        }
        return value;
    }

    /**
     * 取得 DataSource 的可读名称，用于 Meter 的 {@code datasource} 标签：
     * <ol>
     *     <li>优先从 JDBC URL 中解析库名（如 {@code jdbc:mysql://host:3306/crs} -> {@code crs}）；</li>
     *     <li>否则用 Druid 自己的 {@link DataSourceProxy#getName()}；</li>
     *     <li>否则反查 Spring 容器中该实例对应的 Bean 名（带缓存）；</li>
     *     <li>都没有则返回 {@code "unknown"}。</li>
     * </ol>
     */
    private String dataSourceName(DataSourceProxy dataSource) {
        if (dataSource == null) {
            return "unknown";
        }
        // 优先用 JDBC URL 中的库名，比 "DataSource-<identityHashCode>" 更可读
        String database = databaseNameFromUrl(dataSource.getUrl());
        if (database != null && database.length() != 0) {
            return database;
        }
        if (dataSource.getName() != null && dataSource.getName().length() != 0) {
            return dataSource.getName();
        }
        String beanName = dataSourceBeanNames.get(dataSource);
        if (beanName != null) {
            return beanName;
        }
        ListableBeanFactory factory = beanFactory;
        if (factory != null) {
            try {
                Map<String, DataSource> dataSources = factory.getBeansOfType(DataSource.class, false, false);
                for (Map.Entry<String, DataSource> entry : dataSources.entrySet()) {
                    if (entry.getValue() == dataSource) {
                        dataSourceBeanNames.putIfAbsent(dataSource, entry.getKey());
                        return entry.getKey();
                    }
                }
            } catch (BeansException ignored) {
                // 反查失败不应影响 JDBC 事件主路径
            }
        }
        return "unknown";
    }

    /**
     * 从 JDBC URL 中解析库名，支持 {@code jdbc:<type>://host[:port]/db[?params]} 形式
     * （MySQL、PostgreSQL 等）。例如 {@code jdbc:mysql://10.108.0.203:3306/crs?useSSL=false} -> {@code crs}。
     * 解析失败（无 {@code ://}、无路径、路径为空）返回 null。
     */
    private static String databaseNameFromUrl(String url) {
        if (url == null || url.length() == 0) {
            return null;
        }
        int scheme = url.indexOf("://");
        if (scheme < 0) {
            return null;
        }
        String rest = url.substring(scheme + 3);
        int slash = rest.indexOf('/');
        if (slash < 0) {
            return null;
        }
        String path = rest.substring(slash + 1);
        int query = path.indexOf('?');
        if (query >= 0) {
            path = path.substring(0, query);
        }
        int semicolon = path.indexOf(';');
        if (semicolon >= 0) {
            path = path.substring(0, semicolon);
        }
        if (path.length() == 0) {
            return null;
        }
        return path;
    }

    /**
     * 解析 {@code max-window} 配置（支持 ms/s/m 后缀），非法或为空时回退到 2 分钟。
     * 该值只影响之后新建 Meter 的分布统计半衰期。
     */
    private Duration maxWindow() {
        return parseMaxWindow(config.getEvents().getMaxWindow());
    }

    /** 解析 max-window，供同包测试直接覆盖所有支持单位和非法输入。 */
    static Duration parseMaxWindow(String value) {
        if (value == null || value.length() == 0) {
            return DEFAULT_MAX_WINDOW;
        }
        String trimmed = value.trim().toLowerCase();
        try {
            Duration window = null;
            if (trimmed.endsWith("ms")) {
                window = Duration.ofMillis(Long.parseLong(trimmed.substring(0, trimmed.length() - 2)));
            } else if (trimmed.endsWith("s")) {
                window = Duration.ofSeconds(Long.parseLong(trimmed.substring(0, trimmed.length() - 1)));
            } else if (trimmed.endsWith("m")) {
                window = Duration.ofMinutes(Long.parseLong(trimmed.substring(0, trimmed.length() - 1)));
            }
            if (window != null && !window.isNegative() && !window.isZero()) return window;
        } catch (NumberFormatException ignored) {
            // 非数字配置回退默认值。
        } catch (ArithmeticException ignored) {
            // 超出 Duration 可表达范围时同样按非法配置处理。
        }
        return DEFAULT_MAX_WINDOW;
    }

    /** 当前 SQL 近似 LRU 容量，非法值仍保留一个身份以保证事件可被采集。 */
    private int maxSqlIdentities() {
        return Math.max(1, config.getEvents().getMaxSqlIdentities());
    }

    /** 当前 URI 近似 LRU 容量，非法值仍保留一个身份以保证事件可被采集。 */
    private int maxUriIdentities() {
        return Math.max(1, config.getEvents().getMaxUriIdentities());
    }

    /** 上限缩小时，命中路径才进入短临界区执行淘汰。 */
    private void trimSqlStatesIfNecessary() {
        if (sqlStates.size() <= maxSqlIdentities()) {
            return;
        }
        synchronized (sqlMeterLock) {
            trimSqlStates(maxSqlIdentities());
        }
    }

    /** 上限缩小时，命中路径才进入短临界区执行淘汰。 */
    private void trimUriMetersIfNecessary() {
        if (uriMeters.size() <= maxUriIdentities()) {
            return;
        }
        synchronized (uriMeterLock) {
            trimUriMeters(maxUriIdentities());
        }
    }

    /** 将 SQL 近似 LRU 缩减到指定容量，并从 Registry 注销被淘汰的 Meter。调用方持有 sqlMeterLock。 */
    private void trimSqlStates(int maximumSize) {
        while (sqlStates.size() > maximumSize) {
            Map.Entry<String, SqlState> oldest = oldestSqlState();
            if (oldest == null || !sqlStates.remove(oldest.getKey(), oldest.getValue())) {
                return;
            }
            // 输出被近似 LRU 淘汰的 SQL identity，便于核对上限控制。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail identity delete: type=sql source=lru sql={} datasource={} sizeAfter={}",
                        oldest.getValue().hash, oldest.getValue().dataSource, sqlStates.size());
            }
            removeSqlMeters(oldest.getValue().meters);
        }
    }

    /** 将 URI 近似 LRU 缩减到指定容量，并从 Registry 注销被淘汰的 Meter。调用方持有 uriMeterLock。 */
    private void trimUriMeters(int maximumSize) {
        while (uriMeters.size() > maximumSize) {
            Map.Entry<String, UriMeters> oldest = oldestUriMeters();
            if (oldest == null || !uriMeters.remove(oldest.getKey(), oldest.getValue())) {
                return;
            }
            // 输出被近似 LRU 淘汰的 URI identity，便于核对上限控制。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail identity delete: type=uri source=lru uri={} sizeAfter={}",
                        oldest.getKey(), uriMeters.size());
            }
            removeUriMeters(oldest.getValue());
        }
    }

    private Map.Entry<String, SqlState> oldestSqlState() {
        Map.Entry<String, SqlState> oldest = null;
        for (Map.Entry<String, SqlState> entry : sqlStates.entrySet()) {
            if (oldest == null || entry.getValue().lastAccessNanos < oldest.getValue().lastAccessNanos) {
                oldest = entry;
            }
        }
        return oldest;
    }

    private Map.Entry<String, UriMeters> oldestUriMeters() {
        Map.Entry<String, UriMeters> oldest = null;
        for (Map.Entry<String, UriMeters> entry : uriMeters.entrySet()) {
            if (oldest == null || entry.getValue().lastAccessNanos < oldest.getValue().lastAccessNanos) {
                oldest = entry;
            }
        }
        return oldest;
    }

    private Timer registerDetailTimer(Timer.Builder builder, String name, Tags tags) {
        if (!isDetailMeter(name)) {
            throw new IllegalArgumentException("not a Druid detail meter: " + name);
        }
        boolean existedBefore = meterRegistry.find(name).tags(tags).meter() != null;
        Timer meter = builder.register(meterRegistry);
        // 记录 Timer 注册结果及是否复用了 Registry 中的同 ID Meter。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail meter add: action=register type=timer registryExistedBefore={} meter={}",
                    existedBefore, meter.getId());
        }
        return meter;
    }

    private DistributionSummary registerDetailSummary(DistributionSummary.Builder builder, String name, Tags tags) {
        if (!isDetailMeter(name)) {
            throw new IllegalArgumentException("not a Druid detail meter: " + name);
        }
        boolean existedBefore = meterRegistry.find(name).tags(tags).meter() != null;
        DistributionSummary meter = builder.register(meterRegistry);
        // 记录 Summary 注册结果及是否复用了 Registry 中的同 ID Meter。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail meter add: action=register type=summary registryExistedBefore={} meter={}",
                    existedBefore, meter.getId());
        }
        return meter;
    }

    private void removeSqlMeters(SqlMeters meters) {
        removeMeterOrDefer(meters.timer, "sql-lru");
        removeMeterOrDefer(meters.affectedRows, "sql-lru");
        removeMeterOrDefer(meters.fetchedRows, "sql-lru");
    }

    private void removeUriMeters(UriMeters meters) {
        removeMeterOrDefer(meters.timer, "uri-lru");
        removeMeterOrDefer(meters.jdbcExecutions, "uri-lru");
        removeMeterOrDefer(meters.jdbcAffectedRows, "uri-lru");
        removeMeterOrDefer(meters.jdbcFetchedRows, "uri-lru");
    }

    /**
     * 计算 SQL 文本的 MD5（小写十六进制串），作为 SQL 的唯一标识哈希。
     * 计算失败时抛出 {@link IllegalStateException}。
     */
    static String calculateSqlMd5(String sql) {
        if (sql == null || sql.trim().length() == 0) return "";
        try {
            byte[] bytes = MessageDigest.getInstance("MD5").digest(sql.getBytes(StandardCharsets.UTF_8));
            StringBuilder out = new StringBuilder(32);
            final char[] hex = "0123456789abcdef".toCharArray();
            for (byte value : bytes) {
                int unsigned = value & 0xff;
                out.append(hex[unsigned >>> 4]).append(hex[unsigned & 0x0f]);
            }
            return out.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 单条 SQL 的运行态：哈希、数据源名、待落盘原文、建好的 Meter（建好后原文置空）。 */
    private static final class SqlState {
        private final String hash;
        private final String dataSource;
        private volatile String sql;
        private volatile SqlMeters meters;
        private volatile long lastAccessNanos;
        private SqlState(String hash, String dataSource, String sql) {
            this.hash = hash; this.dataSource = dataSource; this.sql = sql; touch();
        }
        private void touch() {
            lastAccessNanos = System.nanoTime();
        }
    }

    /** 某条 SQL 对应的三个 Meter。 */
    private static final class SqlMeters {
        private final Timer timer; private final DistributionSummary affectedRows; private final DistributionSummary fetchedRows;
        private SqlMeters(Timer timer, DistributionSummary affectedRows, DistributionSummary fetchedRows) {
            this.timer = timer; this.affectedRows = affectedRows; this.fetchedRows = fetchedRows;
        }
    }

    /** 某个 URI 对应的四个 Meter。 */
    private static final class UriMeters {
        private final Timer timer; private final DistributionSummary jdbcExecutions; private final DistributionSummary jdbcAffectedRows; private final DistributionSummary jdbcFetchedRows;
        private volatile long lastAccessNanos;
        private UriMeters(Timer timer, DistributionSummary jdbcExecutions, DistributionSummary jdbcAffectedRows, DistributionSummary jdbcFetchedRows) {
            this.timer = timer; this.jdbcExecutions = jdbcExecutions; this.jdbcAffectedRows = jdbcAffectedRows; this.jdbcFetchedRows = jdbcFetchedRows; touch();
        }
        private void touch() {
            lastAccessNanos = System.nanoTime();
        }
    }

    /** 落盘线程工厂，创建名为 {@code druid-sql-mapping-writer} 的守护线程。 */
    private static final class MappingThreadFactory implements java.util.concurrent.ThreadFactory {
        @Override public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "druid-sql-mapping-writer"); thread.setDaemon(true); return thread;
        }
    }
}
