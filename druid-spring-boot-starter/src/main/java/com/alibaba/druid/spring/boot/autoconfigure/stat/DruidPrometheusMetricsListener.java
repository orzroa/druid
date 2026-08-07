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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

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
    private static final Logger LOG = LoggerFactory.getLogger(DruidPrometheusMetricsListener.class);
    /** Spring MVC 在请求属性中存放“最佳匹配路径模板”的 key，例如 {@code /users/{id}}。 */
    private static final String SPRING_MVC_PATTERN_ATTRIBUTE =
            "org.springframework.web.servlet.HandlerMapping.bestMatchingPattern";

    /** 当前的 Prometheus 配置快照（运行时可被 {@link #refresh} 原子替换）。 */
    private volatile DruidStatProperties.Prometheus config;
    /** Last successfully observed values, kept separately so in-place Apollo rebinding can be detected. */
    private volatile DruidStatProperties.Prometheus appliedConfig;
    private final Object refreshLock = new Object();
    /** 应用已有的 MeterRegistry 提供者（Spring 注入，延迟到 init 时取出）。 */
    private final ObjectProvider<MeterRegistry> meterRegistryProvider;
    /** 可选的 URI 模板解析器（SPI），用于把原始 URI 解析为模板以压缩基数。 */
    private final ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider;
    /** Structured event output. The default is deliberately silent to avoid leaking JSON into root logs. */
    private final DruidMetricsEventSink eventSink;

    /**
     * 以 {@code (sqlMd5 + '\u0000' + dataSourceName)} 为 key 缓存的 SQL 状态。
     * 首次见到的 SQL 会创建 Meter，之后再出现则复用已有 Meter。
     */
    private final ConcurrentMap<String, SqlState> sqlStates = new ConcurrentHashMap<String, SqlState>();
    /** 以 URI 模板为 key 缓存的 URI 指标 Meter。 */
    private final ConcurrentMap<String, UriMeters> uriMeters = new ConcurrentHashMap<String, UriMeters>();
    /** 仅记录由本 Listener 注册的 Meter，防止淘汰或失败回收误删其他生产者的同名 Meter。 */
    private final ConcurrentMap<Meter.Id, Meter> ownedMeters = new ConcurrentHashMap<Meter.Id, Meter>();
    /** 仅序列注册/淘汰时使用，正常命中路径不获取此锁。 */
    private final Object sqlMeterLock = new Object();
    /** 仅序列注册/淘汰时使用，正常命中路径不获取此锁。 */
    private final Object uriMeterLock = new Object();
    /** 记录 DataSourceProxy 实例到其 Spring Bean 名称的映射，避免反复扫描容器。 */
    private final ConcurrentMap<DataSourceProxy, String> dataSourceBeanNames =
            new ConcurrentHashMap<DataSourceProxy, String>();
    /** Phase-two SQL aggregate meters, keyed only by stable datasource name. */
    private final ConcurrentMap<String, AggregateSqlMeters> aggregateSqlMeters =
            new ConcurrentHashMap<String, AggregateSqlMeters>();

    /** 实际取出并缓存的 MeterRegistry；为 null 时表示未启用指标，所有事件都会被跳过。 */
    private volatile MeterRegistry meterRegistry;
    /** Phase-two URI aggregate meters have no business labels and are registered once. */
    private volatile AggregateUriMeters aggregateUriMeters;
    /** 单线程、有界队列的 SQL 映射落盘执行器。 */
    private volatile ThreadPoolExecutor mappingExecutor;
    /** 容器工厂（用于反查 DataSource Bean 名）；非 ListableBeanFactory 时为 null。 */
    private volatile ListableBeanFactory beanFactory;

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
        this(config, meterRegistryProvider, uriTemplateResolverProvider, DruidMetricsEventSink.NOOP);
    }

    public DruidPrometheusMetricsListener(DruidStatProperties.Prometheus config,
                                           ObjectProvider<MeterRegistry> meterRegistryProvider,
                                           ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider,
                                           DruidMetricsEventSink eventSink) {
        rejectInvalidThresholds(new DruidStatProperties.Prometheus().getThresholds(), config.getThresholds());
        this.config = config;
        this.appliedConfig = copyConfig(config);
        this.meterRegistryProvider = meterRegistryProvider;
        this.uriTemplateResolverProvider = uriTemplateResolverProvider;
        this.eventSink = eventSink == null ? DruidMetricsEventSink.NOOP : eventSink;
    }

    /**
     * 初始化：取出 MeterRegistry，并在可用时创建落盘线程和事件监听注册。
     * 若容器中没有 MeterRegistry，则直接返回（指标功能视为关闭）。
     */
    @PostConstruct
    public void init() {
        meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry != null) {
            aggregateUriMeters = createAggregateUriMeters();
        }
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
        eventSink.close();
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
        if (config == null) return;
        synchronized (refreshLock) {
            if (this.config == config && sameConfiguration(appliedConfig, config)) return;
            DruidStatProperties.Prometheus previous = appliedConfig;
            rejectInvalidThresholds(previous.getThresholds(), config.getThresholds());
            if (!eventSink.refresh(previous, config)) {
                logRejectedAppenderChanges(previous.getLogging(), config.getLogging());
                retainPreviousAppenderConfiguration(previous.getLogging(), config.getLogging());
            }
            this.config = config;
            logConfigurationChanges(previous, config);
            appliedConfig = copyConfig(config);
        }
    }

    private DruidStatProperties.Prometheus currentConfig() {
        DruidStatProperties.Prometheus current = config;
        if (!sameConfiguration(appliedConfig, current)) refresh(current);
        return config;
    }

    private void retainPreviousAppenderConfiguration(DruidStatProperties.Prometheus.Logging previous,
                                                      DruidStatProperties.Prometheus.Logging current) {
        current.setAutoConfigure(previous.isAutoConfigure());
        current.setDirectory(previous.getDirectory());
        current.setFileName(previous.getFileName());
        current.setQueueSize(previous.getQueueSize());
        current.setMaxFileSize(previous.getMaxFileSize());
        current.setMaxHistoryDays(previous.getMaxHistoryDays());
        current.setTotalSizeCap(previous.getTotalSizeCap());
        current.setShutdownFlushTimeout(previous.getShutdownFlushTimeout());
    }

    private void logRejectedAppenderChanges(DruidStatProperties.Prometheus.Logging previous,
                                             DruidStatProperties.Prometheus.Logging current) {
        logRejectedChange("logging.auto-configure", previous.isAutoConfigure(), current.isAutoConfigure());
        logRejectedChange("logging.directory", previous.getDirectory(), current.getDirectory());
        logRejectedChange("logging.file-name", previous.getFileName(), current.getFileName());
        logRejectedChange("logging.queue-size", previous.getQueueSize(), current.getQueueSize());
        logRejectedChange("logging.max-file-size", previous.getMaxFileSize(), current.getMaxFileSize());
        logRejectedChange("logging.max-history-days", previous.getMaxHistoryDays(), current.getMaxHistoryDays());
        logRejectedChange("logging.total-size-cap", previous.getTotalSizeCap(), current.getTotalSizeCap());
        logRejectedChange("logging.shutdown-flush-timeout", previous.getShutdownFlushTimeout(),
                current.getShutdownFlushTimeout());
    }

    private void logRejectedChange(String property, Object retained, Object rejected) {
        if (retained == null ? rejected == null : retained.equals(rejected)) return;
        LOG.warn("Druid Prometheus config rejected: property={}, rejected={}, retained={}, reason=appender-rebuild-failed",
                property, rejected, retained);
    }

    private void rejectInvalidThresholds(DruidStatProperties.Prometheus.Thresholds previous,
                                         DruidStatProperties.Prometheus.Thresholds current) {
        if (current.getSlowSqlMillis() < 0L) {
            rejectThreshold("thresholds.slow-sql-millis", current.getSlowSqlMillis(), previous.getSlowSqlMillis());
            current.setSlowSqlMillis(previous.getSlowSqlMillis());
        }
        if (current.getLargeSqlReadRows() < 0L) {
            rejectThreshold("thresholds.large-sql-read-rows", current.getLargeSqlReadRows(), previous.getLargeSqlReadRows());
            current.setLargeSqlReadRows(previous.getLargeSqlReadRows());
        }
        if (current.getLargeSqlWriteRows() < 0L) {
            rejectThreshold("thresholds.large-sql-write-rows", current.getLargeSqlWriteRows(), previous.getLargeSqlWriteRows());
            current.setLargeSqlWriteRows(previous.getLargeSqlWriteRows());
        }
        if (current.getSlowUriMillis() < 0L) {
            rejectThreshold("thresholds.slow-uri-millis", current.getSlowUriMillis(), previous.getSlowUriMillis());
            current.setSlowUriMillis(previous.getSlowUriMillis());
        }
        if (current.getLargeUriReadRows() < 0L) {
            rejectThreshold("thresholds.large-uri-read-rows", current.getLargeUriReadRows(), previous.getLargeUriReadRows());
            current.setLargeUriReadRows(previous.getLargeUriReadRows());
        }
        if (current.getLargeUriWriteRows() < 0L) {
            rejectThreshold("thresholds.large-uri-write-rows", current.getLargeUriWriteRows(), previous.getLargeUriWriteRows());
            current.setLargeUriWriteRows(previous.getLargeUriWriteRows());
        }
        if (current.getLargeUriSqlExecutions() < 0L) {
            rejectThreshold("thresholds.large-uri-sql-executions", current.getLargeUriSqlExecutions(),
                    previous.getLargeUriSqlExecutions());
            current.setLargeUriSqlExecutions(previous.getLargeUriSqlExecutions());
        }
    }

    private void rejectThreshold(String property, long rejected, long retained) {
        LOG.warn("Druid Prometheus config rejected: property={}, rejected={}, retained={}, reason=negative-value",
                property, rejected, retained);
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

    @Override
    public void onSqlExecute(String sql, DataSourceProxy dataSource, long durationNanos, Throwable error) {
        DruidStatProperties.Prometheus current = currentConfig();
        if (!isGlobalEnabled(current) || sql == null || sql.length() == 0) return;
        String dataSourceName = dataSourceName(dataSource);
        long safeNanos = Math.max(0L, durationNanos);
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(safeNanos);
        long threshold = current.getThresholds().getSlowSqlMillis();
        boolean slow = threshold > 0L && durationMillis >= threshold;
        AggregateSqlMeters aggregate = aggregateSqlMeters(dataSourceName);
        if (aggregate != null) {
            aggregate.execution.record(safeNanos, TimeUnit.NANOSECONDS);
            if (slow) aggregate.slow.increment();
        }
        if (isPhaseOneEnabled(current) && error == null) {
            SqlState state = sqlState(sql, dataSourceName, current);
            if (state != null && state.meters != null) state.meters.timer.record(safeNanos, TimeUnit.NANOSECONDS);
        }
        boolean abnormal = error != null || slow;
        if (shouldLog(current, abnormal)) {
            String hash = calculateSqlMd5(sql);
            submitMapping(hash, sql, current);
            eventSink.log(sqlExecuteJson(dataSourceName, hash, durationMillis, error != null), abnormal);
        }
    }

    @Override
    public void onSqlUpdateCount(String sql, DataSourceProxy dataSource, int updateCount) {
        DruidStatProperties.Prometheus current = currentConfig();
        if (!isGlobalEnabled(current) || sql == null || sql.length() == 0 || updateCount < 0) return;
        String dataSourceName = dataSourceName(dataSource);
        long threshold = current.getThresholds().getLargeSqlWriteRows();
        boolean large = threshold > 0L && updateCount >= threshold;
        AggregateSqlMeters aggregate = aggregateSqlMeters(dataSourceName);
        if (aggregate != null && large) aggregate.largeWrite.increment();
        if (isPhaseOneEnabled(current)) {
            SqlState state = sqlState(sql, dataSourceName, current);
            if (state != null && state.meters != null) state.meters.affectedRows.record(updateCount);
        }
        if (shouldLog(current, large)) {
            String hash = calculateSqlMd5(sql);
            submitMapping(hash, sql, current);
            eventSink.log(sqlRowsJson("sql_write", dataSourceName, hash, updateCount), large);
        }
    }

    @Override
    public void onSqlResultSetClose(String sql, DataSourceProxy dataSource, int fetchRowCount) {
        DruidStatProperties.Prometheus current = currentConfig();
        if (!isGlobalEnabled(current) || sql == null || sql.length() == 0 || fetchRowCount < 0) return;
        String dataSourceName = dataSourceName(dataSource);
        long threshold = current.getThresholds().getLargeSqlReadRows();
        boolean large = threshold > 0L && fetchRowCount >= threshold;
        AggregateSqlMeters aggregate = aggregateSqlMeters(dataSourceName);
        if (aggregate != null && large) aggregate.largeRead.increment();
        if (isPhaseOneEnabled(current)) {
            SqlState state = sqlState(sql, dataSourceName, current);
            if (state != null && state.meters != null) state.meters.fetchedRows.record(fetchRowCount);
        }
        if (shouldLog(current, large)) {
            String hash = calculateSqlMd5(sql);
            submitMapping(hash, sql, current);
            eventSink.log(sqlRowsJson("sql_read", dataSourceName, hash, fetchRowCount), large);
        }
    }

    @Override
    public void onWebRequest(HttpServletRequest request, String uri, long durationNanos,
                             long jdbcExecuteCount, long jdbcUpdateCount,
                             long jdbcFetchRowCount, Throwable error) {
        DruidStatProperties.Prometheus current = currentConfig();
        if (!isGlobalEnabled(current)) return;
        long safeNanos = Math.max(0L, durationNanos);
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(safeNanos);
        DruidStatProperties.Prometheus.Thresholds thresholds = current.getThresholds();
        boolean slow = thresholds.getSlowUriMillis() > 0L && durationMillis >= thresholds.getSlowUriMillis();
        boolean largeRead = thresholds.getLargeUriReadRows() > 0L && jdbcFetchRowCount >= thresholds.getLargeUriReadRows();
        boolean largeWrite = thresholds.getLargeUriWriteRows() > 0L && jdbcUpdateCount >= thresholds.getLargeUriWriteRows();
        boolean largeSql = thresholds.getLargeUriSqlExecutions() > 0L
                && jdbcExecuteCount >= thresholds.getLargeUriSqlExecutions();
        AggregateUriMeters aggregate = aggregateUriMeters;
        if (aggregate != null) {
            aggregate.request.record(safeNanos, TimeUnit.NANOSECONDS);
            if (slow) aggregate.slow.increment();
            if (largeRead) aggregate.largeRead.increment();
            if (largeWrite) aggregate.largeWrite.increment();
            if (largeSql) aggregate.largeSqlExecutions.increment();
        }
        if (isPhaseOneEnabled(current)) {
            String template = resolveUri(request, uri, current);
            if (template != null && template.length() != 0) {
                UriMeters meters = uriMeters(template);
                if (meters != null) {
                    meters.timer.record(safeNanos, TimeUnit.NANOSECONDS);
                    if (jdbcExecuteCount >= 0) meters.jdbcExecutions.record(jdbcExecuteCount);
                    if (jdbcUpdateCount >= 0) meters.jdbcAffectedRows.record(jdbcUpdateCount);
                    if (jdbcFetchRowCount >= 0) meters.jdbcFetchedRows.record(jdbcFetchRowCount);
                }
            }
        }
        boolean abnormal = error != null || slow || largeRead || largeWrite || largeSql;
        if (shouldLog(current, abnormal)) {
            eventSink.log(uriJson(uri, durationMillis, jdbcExecuteCount, jdbcUpdateCount,
                    jdbcFetchRowCount, error != null), abnormal);
        }
    }

    private boolean isGlobalEnabled(DruidStatProperties.Prometheus current) {
        return current != null && current.isEnabled();
    }

    private boolean isPhaseOneEnabled(DruidStatProperties.Prometheus current) {
        return meterRegistry != null && isGlobalEnabled(current) && current.getEvents().isEnabled();
    }

    /**
     * 取得（必要时懒创建）某个 SQL 的 {@link SqlState}。
     * 内部维护近似 LRU 缓存。正常命中仅更新时间戳，不获取互斥锁；超过
     * {@code max-sql-identities} 上限时，
     * 淘汰最久未使用的 Meter，并为当前 SQL 创建新的 Meter。
     *
     * @return SQL 状态；无法处理时返回 null
     */
    private SqlState sqlState(String sql, String dataSourceName, DruidStatProperties.Prometheus currentConfig) {
        String hash = calculateSqlMd5(sql);
        if (hash.length() == 0) {
            return null;
        }
        String key = hash + '\u0000' + dataSourceName;
        SqlState current = sqlStates.get(key);
        if (current != null) {
            current.touch();
            trimSqlStatesIfNecessary();
            return current;
        }
        synchronized (sqlMeterLock) {
            current = sqlStates.get(key);
            if (current != null) {
                current.touch();
                trimSqlStates(maxSqlIdentities());
                return current;
            }
            SqlState candidate = new SqlState(hash, dataSourceName, sql);
            try {
                candidate.meters = createSqlMeters(candidate.hash, candidate.dataSource);
            } catch (RuntimeException e) {
                return null;
            }
            trimSqlStates(maxSqlIdentities() - 1);
            sqlStates.put(key, candidate);
            if (currentConfig.getLogging().isEnabled() || currentConfig.getSqlMapping().isEnabled()) {
                submitMapping(candidate.hash, candidate.sql, currentConfig);
            }
            candidate.sql = null;
            return candidate;
        }
    }

    /** 获取 URI 的近似 LRU Meter；新 URI 会淘汰最久未使用的 Meter。 */
    private UriMeters uriMeters(String template) {
        UriMeters meters = uriMeters.get(template);
        if (meters != null) {
            meters.touch();
            trimUriMetersIfNecessary();
            return meters;
        }
        synchronized (uriMeterLock) {
            meters = uriMeters.get(template);
            if (meters != null) {
                meters.touch();
                trimUriMeters(maxUriIdentities());
                return meters;
            }
            try {
                meters = createUriMeters(template);
            } catch (RuntimeException e) {
                return null;
            }
            trimUriMeters(maxUriIdentities() - 1);
            uriMeters.put(template, meters);
            return meters;
        }
    }

    /**
     * 把“写 SQL 映射文件”的任务提交给落盘线程（Meter 已在 {@link #sqlState} 中同步创建）。
     * 若线程不可用或队列已满，仅跳过落盘，不丢弃已创建的 Meter，避免丢失观测。
     */
    private void submitMapping(final String hash, final String sql,
                               DruidStatProperties.Prometheus currentConfig) {
        if (hash == null || hash.length() == 0 || sql == null || sql.length() == 0) return;
        boolean requiredByLogging = currentConfig.getLogging().isEnabled();
        if (!requiredByLogging && !currentConfig.getSqlMapping().isEnabled()) return;
        ThreadPoolExecutor executor = mappingExecutor;
        if (executor == null) return;
        final String directory = currentConfig.getLogging().getDirectory();
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        writeSqlMapping(hash, sql, directory);
                    } catch (RuntimeException ignored) {
                        // Mapping is diagnostic data; failures must never affect SQL execution.
                    }
                }
            });
        } catch (RuntimeException e) {
            // Bounded queue full: a later emitted event may retry the same mapping.
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
            timer = registerTimer(Timer.builder("druid.sql.execution.duration")
                    .tags(tags).distributionStatisticExpiry(expiry)
                    .distributionStatisticBufferLength(2), "druid.sql.execution.duration", tags);
            affected = registerSummary(DistributionSummary.builder("druid.sql.affected.rows")
                    .tags(tags).distributionStatisticExpiry(expiry)
                    .distributionStatisticBufferLength(2), "druid.sql.affected.rows", tags);
            fetched = registerSummary(DistributionSummary.builder("druid.sql.fetched.rows")
                    .tags(tags).distributionStatisticExpiry(expiry)
                    .distributionStatisticBufferLength(2), "druid.sql.fetched.rows", tags);
            return new SqlMeters(timer, affected, fetched);
        } catch (RuntimeException e) {
            removeMeter(timer);
            removeMeter(affected);
            removeMeter(fetched);
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
            timer = registerTimer(Timer.builder("druid.uri.request.duration").tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    "druid.uri.request.duration", tags);
            executions = registerSummary(DistributionSummary.builder("druid.uri.jdbc.executions").tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    "druid.uri.jdbc.executions", tags);
            affectedRows = registerSummary(DistributionSummary.builder("druid.uri.jdbc.affected.rows").tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    "druid.uri.jdbc.affected.rows", tags);
            fetchedRows = registerSummary(DistributionSummary.builder("druid.uri.jdbc.fetched.rows").tags(tags)
                    .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2),
                    "druid.uri.jdbc.fetched.rows", tags);
            return new UriMeters(timer, executions, affectedRows, fetchedRows);
        } catch (RuntimeException e) {
            removeMeter(timer);
            removeMeter(executions);
            removeMeter(affectedRows);
            removeMeter(fetchedRows);
            throw e;
        }
    }

    private AggregateSqlMeters aggregateSqlMeters(String dataSource) {
        MeterRegistry registry = meterRegistry;
        if (registry == null) return null;
        AggregateSqlMeters existing = aggregateSqlMeters.get(dataSource);
        if (existing != null) return existing;
        try {
            Tags tags = Tags.of("datasource", dataSource);
            AggregateSqlMeters created = new AggregateSqlMeters(
                    Timer.builder("druid.agg.sql.execution.duration").tags(tags).register(registry),
                    Counter.builder("druid.agg.sql.slow").tags(tags).register(registry),
                    Counter.builder("druid.agg.sql.large.read").tags(tags).register(registry),
                    Counter.builder("druid.agg.sql.large.write").tags(tags).register(registry));
            AggregateSqlMeters raced = aggregateSqlMeters.putIfAbsent(dataSource, created);
            return raced == null ? created : raced;
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private AggregateUriMeters createAggregateUriMeters() {
        try {
            return new AggregateUriMeters(
                    Timer.builder("druid.agg.uri.request.duration").register(meterRegistry),
                    Counter.builder("druid.agg.uri.slow").register(meterRegistry),
                    Counter.builder("druid.agg.uri.large.read").register(meterRegistry),
                    Counter.builder("druid.agg.uri.large.write").register(meterRegistry),
                    Counter.builder("druid.agg.uri.large.sql.executions").register(meterRegistry));
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private boolean shouldLog(DruidStatProperties.Prometheus current, boolean abnormal) {
        if (!current.getLogging().isEnabled() || !eventSink.isActive()) return false;
        if (abnormal) return true;
        double rate = current.getLogging().getNormalSampleRate();
        if (Double.isNaN(rate) || rate <= 0D) return false;
        return rate >= 1D || ThreadLocalRandom.current().nextDouble() < rate;
    }

    private static String sqlExecuteJson(String dataSource, String hash, long durationMillis, boolean error) {
        return new StringBuilder(160)
                .append("{\"timestamp\":\"").append(jsonEscape(OffsetDateTime.now().toString()))
                .append("\",\"event\":\"sql_execute\",\"datasource\":\"").append(jsonEscape(dataSource))
                .append("\",\"sql_md5\":\"").append(jsonEscape(hash))
                .append("\",\"duration_ms\":").append(durationMillis)
                .append(",\"error\":").append(error).append('}').toString();
    }

    private static String sqlRowsJson(String event, String dataSource, String hash, long rows) {
        return new StringBuilder(144)
                .append("{\"timestamp\":\"").append(jsonEscape(OffsetDateTime.now().toString()))
                .append("\",\"event\":\"").append(event)
                .append("\",\"datasource\":\"").append(jsonEscape(dataSource))
                .append("\",\"sql_md5\":\"").append(jsonEscape(hash))
                .append("\",\"rows\":").append(rows).append('}').toString();
    }

    private static String uriJson(String uri, long durationMillis, long sqlCount,
                                  long writeRows, long readRows, boolean error) {
        return new StringBuilder(192)
                .append("{\"timestamp\":\"").append(jsonEscape(OffsetDateTime.now().toString()))
                .append("\",\"event\":\"uri\",\"uri\":\"").append(jsonEscape(uri))
                .append("\",\"duration_ms\":").append(durationMillis)
                .append(",\"sql_count\":").append(sqlCount)
                .append(",\"write_rows\":").append(writeRows)
                .append(",\"read_rows\":").append(readRows)
                .append(",\"error\":").append(error).append('}').toString();
    }

    static String jsonEscape(String value) {
        if (value == null) return "";
        StringBuilder escaped = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\': escaped.append("\\\\"); break;
                case '"': escaped.append("\\\""); break;
                case '\n': escaped.append("\\n"); break;
                case '\r': escaped.append("\\r"); break;
                case '\t': escaped.append("\\t"); break;
                case '\b': escaped.append("\\b"); break;
                case '\f': escaped.append("\\f"); break;
                default:
                    if (ch < 0x20) {
                        String hex = Integer.toHexString(ch);
                        escaped.append("\\u");
                        for (int j = hex.length(); j < 4; j++) escaped.append('0');
                        escaped.append(hex);
                    } else {
                        escaped.append(ch);
                    }
            }
        }
        return escaped.toString();
    }

    private static DruidStatProperties.Prometheus copyConfig(DruidStatProperties.Prometheus source) {
        DruidStatProperties.Prometheus copy = new DruidStatProperties.Prometheus();
        copy.setEnabled(source.isEnabled());
        copy.getEvents().setEnabled(source.getEvents().isEnabled());
        copy.getEvents().setMaxSqlIdentities(source.getEvents().getMaxSqlIdentities());
        copy.getEvents().setMaxUriIdentities(source.getEvents().getMaxUriIdentities());
        copy.getEvents().setMaxWindow(source.getEvents().getMaxWindow());
        copy.getSqlMapping().setEnabled(source.getSqlMapping().isEnabled());
        copy.getSqlMapping().setDirectory(source.getSqlMapping().getDirectory());
        copy.getSqlMapping().setQueueSize(source.getSqlMapping().getQueueSize());
        copy.getUriTemplate().setIncludeContextPath(source.getUriTemplate().isIncludeContextPath());
        copyThresholds(source.getThresholds(), copy.getThresholds());
        copyLogging(source.getLogging(), copy.getLogging());
        return copy;
    }

    private static void copyThresholds(DruidStatProperties.Prometheus.Thresholds source,
                                       DruidStatProperties.Prometheus.Thresholds target) {
        target.setSlowSqlMillis(source.getSlowSqlMillis());
        target.setLargeSqlReadRows(source.getLargeSqlReadRows());
        target.setLargeSqlWriteRows(source.getLargeSqlWriteRows());
        target.setSlowUriMillis(source.getSlowUriMillis());
        target.setLargeUriReadRows(source.getLargeUriReadRows());
        target.setLargeUriWriteRows(source.getLargeUriWriteRows());
        target.setLargeUriSqlExecutions(source.getLargeUriSqlExecutions());
    }

    private static void copyLogging(DruidStatProperties.Prometheus.Logging source,
                                    DruidStatProperties.Prometheus.Logging target) {
        target.setEnabled(source.isEnabled());
        target.setAutoConfigure(source.isAutoConfigure());
        target.setNormalSampleRate(source.getNormalSampleRate());
        target.setDirectory(source.getDirectory());
        target.setFileName(source.getFileName());
        target.setQueueSize(source.getQueueSize());
        target.setMaxFileSize(source.getMaxFileSize());
        target.setMaxHistoryDays(source.getMaxHistoryDays());
        target.setTotalSizeCap(source.getTotalSizeCap());
        target.setShutdownFlushTimeout(source.getShutdownFlushTimeout());
    }

    private static boolean sameConfiguration(DruidStatProperties.Prometheus first,
                                             DruidStatProperties.Prometheus second) {
        if (first == second) return true;
        if (first == null || second == null) return false;
        return first.isEnabled() == second.isEnabled()
                && first.getEvents().isEnabled() == second.getEvents().isEnabled()
                && first.getEvents().getMaxSqlIdentities() == second.getEvents().getMaxSqlIdentities()
                && first.getEvents().getMaxUriIdentities() == second.getEvents().getMaxUriIdentities()
                && equal(first.getEvents().getMaxWindow(), second.getEvents().getMaxWindow())
                && first.getSqlMapping().isEnabled() == second.getSqlMapping().isEnabled()
                && equal(first.getSqlMapping().getDirectory(), second.getSqlMapping().getDirectory())
                && first.getSqlMapping().getQueueSize() == second.getSqlMapping().getQueueSize()
                && first.getUriTemplate().isIncludeContextPath()
                    == second.getUriTemplate().isIncludeContextPath()
                && sameThresholds(first.getThresholds(), second.getThresholds())
                && sameLogging(first.getLogging(), second.getLogging());
    }

    private static boolean sameThresholds(DruidStatProperties.Prometheus.Thresholds first,
                                          DruidStatProperties.Prometheus.Thresholds second) {
        return first.getSlowSqlMillis() == second.getSlowSqlMillis()
                && first.getLargeSqlReadRows() == second.getLargeSqlReadRows()
                && first.getLargeSqlWriteRows() == second.getLargeSqlWriteRows()
                && first.getSlowUriMillis() == second.getSlowUriMillis()
                && first.getLargeUriReadRows() == second.getLargeUriReadRows()
                && first.getLargeUriWriteRows() == second.getLargeUriWriteRows()
                && first.getLargeUriSqlExecutions() == second.getLargeUriSqlExecutions();
    }

    private static boolean sameLogging(DruidStatProperties.Prometheus.Logging first,
                                       DruidStatProperties.Prometheus.Logging second) {
        return first.isEnabled() == second.isEnabled()
                && first.isAutoConfigure() == second.isAutoConfigure()
                && Double.compare(first.getNormalSampleRate(), second.getNormalSampleRate()) == 0
                && equal(first.getDirectory(), second.getDirectory())
                && equal(first.getFileName(), second.getFileName())
                && first.getQueueSize() == second.getQueueSize()
                && equal(first.getMaxFileSize(), second.getMaxFileSize())
                && first.getMaxHistoryDays() == second.getMaxHistoryDays()
                && equal(first.getTotalSizeCap(), second.getTotalSizeCap())
                && equal(first.getShutdownFlushTimeout(), second.getShutdownFlushTimeout());
    }

    private static boolean equal(Object first, Object second) {
        return first == null ? second == null : first.equals(second);
    }

    private void logConfigurationChanges(DruidStatProperties.Prometheus previous,
                                         DruidStatProperties.Prometheus current) {
        if (previous == null) return;
        logChange("enabled", previous.isEnabled(), current.isEnabled(), "next-event");
        logChange("events.enabled", previous.getEvents().isEnabled(), current.getEvents().isEnabled(), "next-event");
        logChange("events.max-sql-identities", previous.getEvents().getMaxSqlIdentities(),
                current.getEvents().getMaxSqlIdentities(), "next-event");
        logChange("events.max-uri-identities", previous.getEvents().getMaxUriIdentities(),
                current.getEvents().getMaxUriIdentities(), "next-event");
        logChange("events.max-window", previous.getEvents().getMaxWindow(),
                current.getEvents().getMaxWindow(), "new-meters");
        logChange("sql-mapping.enabled", previous.getSqlMapping().isEnabled(),
                current.getSqlMapping().isEnabled(), "next-event");
        logChange("sql-mapping.directory", previous.getSqlMapping().getDirectory(),
                current.getSqlMapping().getDirectory(), "ignored-use-logging.directory");
        logChange("sql-mapping.queue-size", previous.getSqlMapping().getQueueSize(),
                current.getSqlMapping().getQueueSize(), "restart-required");
        logChange("uri-template.include-context-path", previous.getUriTemplate().isIncludeContextPath(),
                current.getUriTemplate().isIncludeContextPath(), "next-event");
        logThresholdChanges(previous.getThresholds(), current.getThresholds());
        logLoggingChanges(previous.getLogging(), current.getLogging());
    }

    private void logThresholdChanges(DruidStatProperties.Prometheus.Thresholds oldValue,
                                     DruidStatProperties.Prometheus.Thresholds newValue) {
        logChange("thresholds.slow-sql-millis", oldValue.getSlowSqlMillis(), newValue.getSlowSqlMillis(), "next-event");
        logChange("thresholds.large-sql-read-rows", oldValue.getLargeSqlReadRows(), newValue.getLargeSqlReadRows(), "next-event");
        logChange("thresholds.large-sql-write-rows", oldValue.getLargeSqlWriteRows(), newValue.getLargeSqlWriteRows(), "next-event");
        logChange("thresholds.slow-uri-millis", oldValue.getSlowUriMillis(), newValue.getSlowUriMillis(), "next-event");
        logChange("thresholds.large-uri-read-rows", oldValue.getLargeUriReadRows(), newValue.getLargeUriReadRows(), "next-event");
        logChange("thresholds.large-uri-write-rows", oldValue.getLargeUriWriteRows(), newValue.getLargeUriWriteRows(), "next-event");
        logChange("thresholds.large-uri-sql-executions", oldValue.getLargeUriSqlExecutions(), newValue.getLargeUriSqlExecutions(), "next-event");
    }

    private void logLoggingChanges(DruidStatProperties.Prometheus.Logging oldValue,
                                   DruidStatProperties.Prometheus.Logging newValue) {
        logChange("logging.enabled", oldValue.isEnabled(), newValue.isEnabled(), "next-event");
        logChange("logging.auto-configure", oldValue.isAutoConfigure(), newValue.isAutoConfigure(), "appender-rebuild");
        logChange("logging.normal-sample-rate", oldValue.getNormalSampleRate(), newValue.getNormalSampleRate(), "next-event");
        logChange("logging.directory", oldValue.getDirectory(), newValue.getDirectory(), "appender-rebuild");
        logChange("logging.file-name", oldValue.getFileName(), newValue.getFileName(), "appender-rebuild");
        logChange("logging.queue-size", oldValue.getQueueSize(), newValue.getQueueSize(), "appender-rebuild");
        logChange("logging.max-file-size", oldValue.getMaxFileSize(), newValue.getMaxFileSize(), "appender-rebuild");
        logChange("logging.max-history-days", oldValue.getMaxHistoryDays(), newValue.getMaxHistoryDays(), "appender-rebuild");
        logChange("logging.total-size-cap", oldValue.getTotalSizeCap(), newValue.getTotalSizeCap(), "appender-rebuild");
        logChange("logging.shutdown-flush-timeout", oldValue.getShutdownFlushTimeout(),
                newValue.getShutdownFlushTimeout(), "next-close");
    }

    private void logChange(String property, Object oldValue, Object newValue, String effect) {
        if (oldValue == null ? newValue == null : oldValue.equals(newValue)) return;
        LOG.info("Druid Prometheus config changed: property={}, old={}, new={}, effect={}",
                property, oldValue, newValue, effect);
    }

    /**
     * 将 SQL 文本按 MD5 文件名落盘，便于后续在 Grafana 等侧把 md5 还原为可读 SQL。
     * 使用原子移动避免并发重复写；已存在同名文件则跳过。
     */
    private void writeSqlMapping(String hash, String sql, String configuredDirectory) {
        Path directory = Paths.get(configuredDirectory == null ? "./logs" : configuredDirectory);
        Path target = directory.resolve("sql_mapping_" + hash + ".log");
        try {
            if (Files.exists(target)) {
                return;
            }
            Files.createDirectories(directory);
            if (Files.exists(target)) {
                return;
            }
            // 先写临时文件再原子重命名，保证目标文件要么完整存在、要么不存在
            Path temporary = Files.createTempFile(directory, "sql_mapping_" + hash, ".log.tmp");
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
    private String resolveUri(HttpServletRequest request, String fallback,
                              DruidStatProperties.Prometheus current) {
        DruidUriTemplateResolver resolver = uriTemplateResolverProvider.getIfAvailable();
        String value = resolver == null ? null : resolver.resolve(request);
        if (value == null || value.length() == 0) {
            Object pattern = request.getAttribute(SPRING_MVC_PATTERN_ATTRIBUTE);
            value = pattern instanceof String ? (String) pattern : null;
        }
        if (value == null || value.length() == 0) {
            value = fallback;
        }
        if (current.getUriTemplate().isIncludeContextPath() && value != null
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

    static Duration parseMaxWindow(String value) {
        if (value == null || value.length() == 0) {
            return Duration.ofMinutes(2);
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
        }
        return Duration.ofMinutes(2);
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

    private Timer registerTimer(Timer.Builder builder, String name, Tags tags) {
        boolean existed = meterRegistry.find(name).tags(tags).meter() != null;
        Timer meter = builder.register(meterRegistry);
        if (!existed) {
            ownedMeters.putIfAbsent(meter.getId(), meter);
        }
        return meter;
    }

    private DistributionSummary registerSummary(DistributionSummary.Builder builder, String name, Tags tags) {
        boolean existed = meterRegistry.find(name).tags(tags).meter() != null;
        DistributionSummary meter = builder.register(meterRegistry);
        if (!existed) {
            ownedMeters.putIfAbsent(meter.getId(), meter);
        }
        return meter;
    }

    private void removeSqlMeters(SqlMeters meters) {
        removeMeter(meters.timer);
        removeMeter(meters.affectedRows);
        removeMeter(meters.fetchedRows);
    }

    private void removeUriMeters(UriMeters meters) {
        removeMeter(meters.timer);
        removeMeter(meters.jdbcExecutions);
        removeMeter(meters.jdbcAffectedRows);
        removeMeter(meters.jdbcFetchedRows);
    }

    /** 仅回收本 Listener 注册的 Meter，回收异常不覆盖原始注册异常。 */
    private void removeMeter(Meter meter) {
        if (meter == null || !ownedMeters.remove(meter.getId(), meter)) {
            return;
        }
        try {
            meterRegistry.remove(meter);
        } catch (RuntimeException ignored) {
            // Meter 注册失败的错误不能影响业务事件路径。
        }
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

    private static final class AggregateSqlMeters {
        private final Timer execution;
        private final Counter slow;
        private final Counter largeRead;
        private final Counter largeWrite;

        private AggregateSqlMeters(Timer execution, Counter slow, Counter largeRead, Counter largeWrite) {
            this.execution = execution;
            this.slow = slow;
            this.largeRead = largeRead;
            this.largeWrite = largeWrite;
        }
    }

    private static final class AggregateUriMeters {
        private final Timer request;
        private final Counter slow;
        private final Counter largeRead;
        private final Counter largeWrite;
        private final Counter largeSqlExecutions;

        private AggregateUriMeters(Timer request, Counter slow, Counter largeRead,
                                   Counter largeWrite, Counter largeSqlExecutions) {
            this.request = request;
            this.slow = slow;
            this.largeRead = largeRead;
            this.largeWrite = largeWrite;
            this.largeSqlExecutions = largeSqlExecutions;
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
