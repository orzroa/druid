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
    /** Spring MVC 在请求属性中存放“最佳匹配路径模板”的 key，例如 {@code /users/{id}}。 */
    private static final String SPRING_MVC_PATTERN_ATTRIBUTE =
            "org.springframework.web.servlet.HandlerMapping.bestMatchingPattern";
    private static final Duration DEFAULT_MAX_WINDOW = Duration.ofMinutes(2);

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
    /** 仅记录由本 Listener 注册的 Meter，防止淘汰或失败回收误删其他生产者的同名 Meter。 */
    private final ConcurrentMap<Meter.Id, Meter> ownedMeters = new ConcurrentHashMap<Meter.Id, Meter>();
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
            if (jdbcExecuteCount >= 0) meters.jdbcExecutions.record(jdbcExecuteCount);
            if (jdbcUpdateCount >= 0) meters.jdbcAffectedRows.record(jdbcUpdateCount);
            if (jdbcFetchRowCount >= 0) meters.jdbcFetchedRows.record(jdbcFetchRowCount);
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

    /**
     * 执行实际的清理：注销 Starter 持有的一期 SQL/URI 明细 Meter，清空 identity/LRU/句柄。
     * 调用方 {@link DruidPrometheusMeterCleanup} 持有写锁。
     * 始终移除 identity/LRU 状态；注销失败的单个 Meter 句柄存入 {@link #failedRemovals}，下周期重试。
     */
    private DruidPrometheusMeterCleanup.CleanupCounts cleanupDetailMeters() {
        int sqlBefore = sqlStates.size();
        int uriBefore = uriMeters.size();

        // 先重试上一周期注销失败的 Meter
        int retrySuccess = 0, retryFailed = 0;
        if (!failedRemovals.isEmpty()) {
            Iterator<Map.Entry<Meter.Id, Meter>> retryIt = failedRemovals.entrySet().iterator();
            while (retryIt.hasNext()) {
                Map.Entry<Meter.Id, Meter> entry = retryIt.next();
                try {
                    meterRegistry.remove(entry.getValue());
                    retryIt.remove();
                    retrySuccess++;
                } catch (RuntimeException e) {
                    retryFailed++;
                    // 保留在 failedRemovals 供下次重试
                }
            }
        }

        // 1. 注销 SQL Meter；始终移除 SqlState，失败 Meter 存入 failedRemovals
        int sqlSuccess = 0, sqlFailed = 0;
        for (SqlState state : sqlStates.values()) {
            if (state.meters == null) continue;
            if (removeMeterOrDefer(state.meters.timer)) sqlSuccess++; else sqlFailed++;
            if (removeMeterOrDefer(state.meters.affectedRows)) sqlSuccess++; else sqlFailed++;
            if (removeMeterOrDefer(state.meters.fetchedRows)) sqlSuccess++; else sqlFailed++;
        }
        sqlStates.clear();

        // 2. 注销 URI Meter；同样始终移除
        int uriSuccess = 0, uriFailed = 0;
        for (UriMeters meters : uriMeters.values()) {
            if (removeMeterOrDefer(meters.timer)) uriSuccess++; else uriFailed++;
            if (removeMeterOrDefer(meters.jdbcExecutions)) uriSuccess++; else uriFailed++;
            if (removeMeterOrDefer(meters.jdbcAffectedRows)) uriSuccess++; else uriFailed++;
            if (removeMeterOrDefer(meters.jdbcFetchedRows)) uriSuccess++; else uriFailed++;
        }
        uriMeters.clear();

        return new DruidPrometheusMeterCleanup.CleanupCounts(sqlBefore, uriBefore,
                sqlSuccess, sqlFailed, uriSuccess, uriFailed, retrySuccess, retryFailed);
    }

    /**
     * 注销单个 Meter；成功返回 true。失败时把句柄存入 {@link #failedRemovals} 供下周期重试，返回 false。
     * 仅当 Meter 属于本 listener（即 {@link #ownedMeters} 中存在对应记录）时才调用 registry.remove；
     * 外部组件预注册的同 ID Meter 不删除，仅清除本地 identity 引用（由调用方负责）。
     */
    private boolean removeMeterOrDefer(Meter meter) {
        if (meter == null) return true;
        // 只有 ownedMeters.remove 成功才说明这个 Meter 归本 listener 管
        if (!ownedMeters.remove(meter.getId(), meter)) {
            // 不属于本 listener（外部预注册），不调用 registry.remove
            return true;
        }
        try {
            meterRegistry.remove(meter);
            return true;
        } catch (RuntimeException e) {
            failedRemovals.put(meter.getId(), meter);
            meterCleanup.warnRemovalFailure(meter.getId(), e);
            return false;
        }
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
            current.touch();
            trimSqlStatesIfNecessary();
            return current;
        }
        // 调用方已持有 meterCleanup 读锁，防止与清理写锁并发
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
            submitMapping(candidate);
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
        // 调用方已持有 meterCleanup 读锁，防止与清理写锁并发
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
