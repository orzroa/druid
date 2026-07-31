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
 * Records native Druid statistic events in the application's MeterRegistry.
 */
public final class DruidPrometheusMetricsListener implements StatFilterEventListener, WebStatEventListener,
        DruidPrometheusMetricsRefresher, BeanFactoryAware {
    private static final Logger LOG = LoggerFactory.getLogger(DruidPrometheusMetricsListener.class);
    private static final String SPRING_MVC_PATTERN_ATTRIBUTE =
            "org.springframework.web.servlet.HandlerMapping.bestMatchingPattern";

    private volatile DruidStatProperties.Prometheus config;
    private final ObjectProvider<MeterRegistry> meterRegistryProvider;
    private final ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider;
    private final ConcurrentMap<String, SqlState> sqlStates = new ConcurrentHashMap<String, SqlState>();
    private final ConcurrentMap<String, UriMeters> uriMeters = new ConcurrentHashMap<String, UriMeters>();
    private final ConcurrentMap<DataSourceProxy, String> dataSourceBeanNames =
            new ConcurrentHashMap<DataSourceProxy, String>();
    private final AtomicInteger sqlIdentityCount = new AtomicInteger();
    private final AtomicInteger uriIdentityCount = new AtomicInteger();
    private final AtomicLong sqlDropped = new AtomicLong();
    private final AtomicLong uriDropped = new AtomicLong();

    private volatile MeterRegistry meterRegistry;
    private volatile ThreadPoolExecutor mappingExecutor;
    private volatile Counter sqlDroppedCounter;
    private volatile Counter uriDroppedCounter;
    private volatile ListableBeanFactory beanFactory;

    public DruidPrometheusMetricsListener(DruidStatProperties.Prometheus config,
                                           ObjectProvider<MeterRegistry> meterRegistryProvider,
                                           ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider) {
        this.config = config;
        this.meterRegistryProvider = meterRegistryProvider;
        this.uriTemplateResolverProvider = uriTemplateResolverProvider;
    }

    @PostConstruct
    public void init() {
        meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry == null) {
            return;
        }
        int queueSize = Math.max(1, config.getSqlMapping().getQueueSize());
        mappingExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueSize), new MappingThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        sqlDroppedCounter = Counter.builder("druid.prometheus.meter.dropped")
                .tags("type", "sql").register(meterRegistry);
        uriDroppedCounter = Counter.builder("druid.prometheus.meter.dropped")
                .tags("type", "uri").register(meterRegistry);
        StatFilterContext.getInstance().addEventListener(this);
        WebStatEventContext.addListener(this);
    }

    @PreDestroy
    public void destroy() {
        StatFilterContext.getInstance().removeEventListener(this);
        WebStatEventContext.removeListener(this);
        ThreadPoolExecutor executor = mappingExecutor;
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public void refresh(DruidStatProperties.Prometheus config) {
        if (config != null) {
            this.config = config;
        }
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof ListableBeanFactory) {
            this.beanFactory = (ListableBeanFactory) beanFactory;
        }
    }

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

    @Override
    public void onWebRequest(HttpServletRequest request, String uri, long durationNanos,
                             long jdbcExecuteCount, long jdbcUpdateCount,
                             long jdbcFetchRowCount, Throwable error) {
        if (!isEnabled()) {
            return;
        }
        String template = resolveUri(request, uri);
        if (template == null || template.length() == 0) {
            return;
        }
        UriMeters meters = uriMeters.get(template);
        if (meters == null) {
            synchronized (uriMeters) {
                meters = uriMeters.get(template);
                if (meters == null) {
                    meters = createUriMeters(template);
                    if (meters == null) {
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

    private boolean isEnabled() {
        return meterRegistry != null && config.isEnabled() && config.getEvents().isEnabled();
    }

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
        if (sqlIdentityCount.incrementAndGet() > Math.max(1, config.getEvents().getMaxSqlIdentities())) {
            sqlIdentityCount.decrementAndGet();
            dropSql();
            return null;
        }
        SqlState candidate = new SqlState(hash, dataSourceName, sql);
        SqlState existing = sqlStates.putIfAbsent(key, candidate);
        if (existing != null) {
            sqlIdentityCount.decrementAndGet();
            return existing;
        }
        submitMapping(candidate, key);
        return candidate;
    }

    private void submitMapping(final SqlState state, final String key) {
        ThreadPoolExecutor executor = mappingExecutor;
        if (executor == null) {
            sqlStates.remove(key, state);
            sqlIdentityCount.decrementAndGet();
            dropSql("mapping executor is unavailable");
            return;
        }
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        writeSqlMapping(state.hash, state.sql);
                        state.meters = createSqlMeters(state.hash, state.dataSource);
                        state.sql = null;
                    } catch (RuntimeException e) {
                        sqlStates.remove(key, state);
                        sqlIdentityCount.decrementAndGet();
                        dropSql("SQL mapping write failed");
                    }
                }
            });
        } catch (RuntimeException e) {
            sqlStates.remove(key, state);
            sqlIdentityCount.decrementAndGet();
            dropSql("SQL mapping queue is full");
        }
    }

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

    private void writeSqlMapping(String hash, String sql) {
        if (!config.getSqlMapping().isEnabled()) {
            return;
        }
        Path directory = Paths.get(config.getSqlMapping().getDirectory());
        Path target = directory.resolve(hash);
        try {
            if (Files.exists(target)) {
                return;
            }
            Files.createDirectories(directory);
            if (Files.exists(target)) {
                return;
            }
            Path temporary = Files.createTempFile(directory, hash, ".tmp");
            try {
                Files.write(temporary, sql.getBytes(StandardCharsets.UTF_8));
                try {
                    try {
                        Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                    } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
                        Files.move(temporary, target);
                    }
                } catch (java.nio.file.FileAlreadyExistsException ignored) {
                    // A different process created the same MD5 mapping first.
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (IOException e) {
            throw new IllegalStateException("write SQL mapping failed", e);
        }
    }

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

    private String dataSourceName(DataSourceProxy dataSource) {
        if (dataSource == null) {
            return "unknown";
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
                // The fallback must not affect the JDBC event path.
            }
        }
        return "unknown";
    }

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

    private void dropSql() {
        dropSql("identity limit " + config.getEvents().getMaxSqlIdentities() + " reached");
    }

    private void dropSql(String reason) {
        long dropped = sqlDropped.incrementAndGet();
        if (sqlDroppedCounter != null) sqlDroppedCounter.increment();
        warnDropped("SQL", dropped, reason);
    }

    private void dropUri() {
        long dropped = uriDropped.incrementAndGet();
        if (uriDroppedCounter != null) uriDroppedCounter.increment();
        warnDropped("URI", dropped, "identity limit " + config.getEvents().getMaxUriIdentities() + " reached");
    }

    private void warnDropped(String type, long dropped, String reason) {
        long step = Math.max(1L, config.getEvents().getLogStep());
        if (dropped == 1L || (dropped - 1L) % step == 0L) {
            LOG.warn("Druid Prometheus {} meter was dropped: {}; dropped {} in total", type, reason, dropped);
        }
    }

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

    private static final class SqlState {
        private final String hash;
        private final String dataSource;
        private volatile String sql;
        private volatile SqlMeters meters;
        private SqlState(String hash, String dataSource, String sql) {
            this.hash = hash; this.dataSource = dataSource; this.sql = sql;
        }
    }
    private static final class SqlMeters {
        private final Timer timer; private final DistributionSummary affectedRows; private final DistributionSummary fetchedRows;
        private SqlMeters(Timer timer, DistributionSummary affectedRows, DistributionSummary fetchedRows) {
            this.timer = timer; this.affectedRows = affectedRows; this.fetchedRows = fetchedRows;
        }
    }
    private static final class UriMeters {
        private final Timer timer; private final DistributionSummary jdbcExecutions; private final DistributionSummary jdbcAffectedRows; private final DistributionSummary jdbcFetchedRows;
        private UriMeters(Timer timer, DistributionSummary jdbcExecutions, DistributionSummary jdbcAffectedRows, DistributionSummary jdbcFetchedRows) {
            this.timer = timer; this.jdbcExecutions = jdbcExecutions; this.jdbcAffectedRows = jdbcAffectedRows; this.jdbcFetchedRows = jdbcFetchedRows;
        }
    }
    private static final class MappingThreadFactory implements java.util.concurrent.ThreadFactory {
        @Override public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "druid-sql-mapping-writer"); thread.setDaemon(true); return thread;
        }
    }
}
