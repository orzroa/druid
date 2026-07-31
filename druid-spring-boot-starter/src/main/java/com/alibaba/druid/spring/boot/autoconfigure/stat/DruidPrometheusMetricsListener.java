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
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
 *     <li>对 SQL / URI 的标签基数（-identity 上限）做了限制，超出后丢弃并计数，防止 Meter 爆炸。</li>
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
    /** 应用已有的 MeterRegistry 提供者（Spring 注入，延迟到 init 时取出）。 */
    private final ObjectProvider<MeterRegistry> meterRegistryProvider;
    /** 可选的 URI 模板解析器（SPI），用于把原始 URI 解析为模板以压缩基数。 */
    private final ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider;

    /**
     * 以 {@code (sqlMd5 + '\u0000' + dataSourceName)} 为 key 缓存的 SQL 状态。
     * 首次见到的 SQL 会异步计算 Meter，之后再出现则复用已有 Meter。
     */
    private final ConcurrentMap<String, SqlState> sqlStates = new ConcurrentHashMap<String, SqlState>();
    /** 以 URI 模板为 key 缓存的 URI 指标 Meter。 */
    private final ConcurrentMap<String, UriMeters> uriMeters = new ConcurrentHashMap<String, UriMeters>();
    /** 记录 DataSourceProxy 实例到其 Spring Bean 名称的映射，避免反复扫描容器。 */
    private final ConcurrentMap<DataSourceProxy, String> dataSourceBeanNames =
            new ConcurrentHashMap<DataSourceProxy, String>();

    /** 已注册的 SQL 身份（不同 (sql, datasource) 组合）计数，用于基数上限控制。 */
    private final AtomicInteger sqlIdentityCount = new AtomicInteger();
    /** 已注册的 URI 身份计数，用于基数上限控制。 */
    private final AtomicInteger uriIdentityCount = new AtomicInteger();
    /** 因超出 SQL 基数上限而被丢弃的观测计数。 */
    private final AtomicLong sqlDropped = new AtomicLong();
    /** 因超出 URI 基数上限而被丢弃的观测计数。 */
    private final AtomicLong uriDropped = new AtomicLong();

    /** 实际取出并缓存的 MeterRegistry；为 null 时表示未启用指标，所有事件都会被跳过。 */
    private volatile MeterRegistry meterRegistry;
    /** 单线程、有界队列的 SQL 映射落盘执行器。 */
    private volatile ThreadPoolExecutor mappingExecutor;
    /** “被丢弃的 SQL 观测”计数器（注册到 MeterRegistry）。 */
    private volatile Counter sqlDroppedCounter;
    /** “被丢弃的 URI 观测”计数器（注册到 MeterRegistry）。 */
    private volatile Counter uriDroppedCounter;
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
        this.config = config;
        this.meterRegistryProvider = meterRegistryProvider;
        this.uriTemplateResolverProvider = uriTemplateResolverProvider;
    }

    /**
     * 初始化：取出 MeterRegistry，并在可用时创建落盘线程、丢弃计数器和事件监听注册。
     * 若容器中没有 MeterRegistry，则直接返回（指标功能视为关闭）。
     */
    @PostConstruct
    public void init() {
        meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry == null) {
            return;
        }
        int queueSize = Math.max(1, config.getSqlMapping().getQueueSize());
        // 单线程 + 有界队列 + 拒绝即抛 AbortPolicy，保证落盘不会无限堆积、也不会阻塞事件线程
        mappingExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueSize), new MappingThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        // 这两个 Counter 用于观测“因基数上限被丢弃”的情况，便于排查指标丢失
        sqlDroppedCounter = Counter.builder("druid.prometheus.meter.dropped")
                .tags("type", "sql").register(meterRegistry);
        uriDroppedCounter = Counter.builder("druid.prometheus.meter.dropped")
                .tags("type", "uri").register(meterRegistry);
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
     * 只替换非 null 的参数；对后续事件立即生效，不影响已注册的 Meter。
     *
     * @param config 新的 Prometheus 配置快照
     */
    @Override
    public void refresh(DruidStatProperties.Prometheus config) {
        if (config != null) {
            this.config = config;
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
        SqlState state = sqlState(sql, dataSource);
        if (state != null && state.meters != null) {
            state.meters.timer.record(durationNanos, TimeUnit.NANOSECONDS);
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
        SqlState state = sqlState(sql, dataSource);
        if (updateCount >= 0 && state != null && state.meters != null) {
            state.meters.affectedRows.record(updateCount);
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
        SqlState state = sqlState(sql, dataSource);
        if (state != null && state.meters != null) {
            state.meters.fetchedRows.record(fetchRowCount);
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
        // 懒创建 URI 对应的 Meter（double-checked 锁保证单例）
        UriMeters meters = uriMeters.get(template);
        if (meters == null) {
            synchronized (uriMeters) {
                meters = uriMeters.get(template);
                if (meters == null) {
                    meters = createUriMeters(template);
                    if (meters == null) {
                        // 达到 URI 基数上限，创建失败
                        dropUri();
                        return;
                    }
                    uriMeters.put(template, meters);
                }
            }
        }
        meters.timer.record(durationNanos, TimeUnit.NANOSECONDS);
        if (jdbcExecuteCount >= 0) meters.jdbcExecutions.record(jdbcExecuteCount);
        if (jdbcUpdateCount >= 0) meters.jdbcAffectedRows.record(jdbcUpdateCount);
        if (jdbcFetchRowCount >= 0) meters.jdbcFetchedRows.record(jdbcFetchRowCount);
    }

    /**
     * 判断指标收集是否在当前配置下启用：必须有 MeterRegistry、且 enabled 与 events.enabled 均为 true。
     */
    private boolean isEnabled() {
        return meterRegistry != null && config.isEnabled() && config.getEvents().isEnabled();
    }

    /**
     * 取得（必要时懒创建）某个 SQL 的 {@link SqlState}。
     * 内部维护 SQL 身份计数，超过 {@code max-sql-identities} 上限时丢弃该观测。
     *
     * @return SQL 状态；若被丢弃或无法处理则返回 null
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
            return current;
        }
        // 超过 SQL 身份上限：撤销计数并丢弃
        if (sqlIdentityCount.incrementAndGet() > Math.max(1, config.getEvents().getMaxSqlIdentities())) {
            sqlIdentityCount.decrementAndGet();
            dropSql();
            return null;
        }
        SqlState candidate = new SqlState(hash, dataSourceName, sql);
        SqlState existing = sqlStates.putIfAbsent(key, candidate);
        if (existing != null) {
            // 并发下被别的线程抢先创建，撤销本地计数
            sqlIdentityCount.decrementAndGet();
            return existing;
        }
        // 同步创建 Meter，使首次执行即可被记录；SQL 文本映射仍异步落盘。
        // Meter 注册只是 MeterRegistry 内部的 ConcurrentHashMap 写入（微秒级），
        // 不会阻塞 JDBC 事件路径；真正耗时的文件落盘仍由 submitMapping 异步完成。
        try {
            candidate.meters = createSqlMeters(candidate.hash, candidate.dataSource);
        } catch (RuntimeException e) {
            sqlStates.remove(key, candidate);
            sqlIdentityCount.decrementAndGet();
            dropSql("SQL meter creation failed");
            return null;
        }
        submitMapping(candidate, key);
        return candidate;
    }

    /**
     * 把“写 SQL 映射文件”的任务提交给落盘线程（Meter 已在 {@link #sqlState} 中同步创建）。
     * 若线程不可用或队列已满，仅跳过落盘，不丢弃已创建的 Meter，避免丢失观测。
     */
    private void submitMapping(final SqlState state, final String key) {
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
        Timer timer = Timer.builder("druid.sql.execution.duration")
                .tags(tags).distributionStatisticExpiry(expiry)
                .distributionStatisticBufferLength(2).register(meterRegistry);
        DistributionSummary affected = DistributionSummary.builder("druid.sql.affected.rows")
                .tags(tags).distributionStatisticExpiry(expiry)
                .distributionStatisticBufferLength(2).register(meterRegistry);
        DistributionSummary fetched = DistributionSummary.builder("druid.sql.fetched.rows")
                .tags(tags).distributionStatisticExpiry(expiry)
                .distributionStatisticBufferLength(2).register(meterRegistry);
        return new SqlMeters(timer, affected, fetched);
    }

    /**
     * 为某个 URI 创建四个 Meter（标签 {@code uri=模板}）：
     * <ul>
     *     <li>{@code druid.uri.request.duration} — 请求耗时（Timer）；</li>
     *     <li>{@code druid.uri.jdbc.executions} — 触发的 JDBC 执行次数；</li>
     *     <li>{@code druid.uri.jdbc.affected.rows} — 影响行数；</li>
     *     <li>{@code druid.uri.jdbc.fetched.rows} — 抓取行数。</li>
     * </ul>
     * 受 {@code max-uri-identities} 上限约束，超出返回 null（由调用方丢弃）。
     */
    private UriMeters createUriMeters(String uri) {
        if (uriIdentityCount.incrementAndGet() > Math.max(1, config.getEvents().getMaxUriIdentities())) {
            uriIdentityCount.decrementAndGet();
            return null;
        }
        Tags tags = Tags.of("uri", uri);
        Duration expiry = maxWindow().dividedBy(2);
        UriMeters meters = new UriMeters(
                Timer.builder("druid.uri.request.duration").tags(tags)
                        .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2)
                        .register(meterRegistry),
                DistributionSummary.builder("druid.uri.jdbc.executions").tags(tags)
                        .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2)
                        .register(meterRegistry),
                DistributionSummary.builder("druid.uri.jdbc.affected.rows").tags(tags)
                        .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2)
                        .register(meterRegistry),
                DistributionSummary.builder("druid.uri.jdbc.fetched.rows").tags(tags)
                        .distributionStatisticExpiry(expiry).distributionStatisticBufferLength(2)
                        .register(meterRegistry));
        return meters;
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
        String value = config.getEvents().getMaxWindow();
        if (value == null || value.length() == 0) {
            return Duration.ofMinutes(2);
        }
        String trimmed = value.trim().toLowerCase();
        try {
            Duration window = null;
            if (trimmed.endsWith("ms")) window = Duration.ofMillis(Long.parseLong(trimmed.substring(0, trimmed.length() - 2)));
            if (trimmed.endsWith("s")) window = Duration.ofSeconds(Long.parseLong(trimmed.substring(0, trimmed.length() - 1)));
            if (trimmed.endsWith("m")) window = Duration.ofMinutes(Long.parseLong(trimmed.substring(0, trimmed.length() - 1)));
            if (window != null && !window.isNegative() && !window.isZero()) return window;
        } catch (NumberFormatException ignored) {
        }
        return Duration.ofMinutes(2);
    }

    /** 因达到 SQL 身份上限而丢弃一次观测。 */
    private void dropSql() {
        dropSql("identity limit " + config.getEvents().getMaxSqlIdentities() + " reached");
    }

    /** 记录一次 SQL 观测被丢弃（计数 + 周期性 WARN 日志）。 */
    private void dropSql(String reason) {
        long dropped = sqlDropped.incrementAndGet();
        if (sqlDroppedCounter != null) sqlDroppedCounter.increment();
        warnDropped("SQL", dropped, reason);
    }

    /** 因达到 URI 身份上限而丢弃一次观测。 */
    private void dropUri() {
        long dropped = uriDropped.incrementAndGet();
        if (uriDroppedCounter != null) uriDroppedCounter.increment();
        warnDropped("URI", dropped, "identity limit " + config.getEvents().getMaxUriIdentities() + " reached");
    }

    /** 按 {@code log-step} 间隔输出丢弃告警，避免高频丢弃刷屏。 */
    private void warnDropped(String type, long dropped, String reason) {
        long step = Math.max(1L, config.getEvents().getLogStep());
        if (dropped == 1L || (dropped - 1L) % step == 0L) {
            LOG.warn("Druid Prometheus {} meter was dropped: {}; dropped {} in total", type, reason, dropped);
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
        private SqlState(String hash, String dataSource, String sql) {
            this.hash = hash; this.dataSource = dataSource; this.sql = sql;
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
        private UriMeters(Timer timer, DistributionSummary jdbcExecutions, DistributionSummary jdbcAffectedRows, DistributionSummary jdbcFetchedRows) {
            this.timer = timer; this.jdbcExecutions = jdbcExecutions; this.jdbcAffectedRows = jdbcAffectedRows; this.jdbcFetchedRows = jdbcFetchedRows;
        }
    }

    /** 落盘线程工厂，创建名为 {@code druid-sql-mapping-writer} 的守护线程。 */
    private static final class MappingThreadFactory implements java.util.concurrent.ThreadFactory {
        @Override public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "druid-sql-mapping-writer"); thread.setDaemon(true); return thread;
        }
    }
}
