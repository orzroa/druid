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

import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import com.alibaba.druid.stat.DruidStatService;
import com.alibaba.druid.support.json.JSONUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.beans.factory.ObjectProvider;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintains Druid metric values in an existing Micrometer registry.
 *
 * <p>Druid JSON endpoints are read once per refresh. Gauge callbacks only read
 * an in-memory value, so an Actuator scrape does not cause repeated JSON
 * serialization or O(N squared) work.</p>
 *
 * @author druid
 */
public final class DruidPrometheusMetricsExporter {
    private static final long REFRESH_INTERVAL_SECONDS = 15;
    private static final String SQL_SERIES_PREFIX = "sql|";
    private static final String URI_SERIES_PREFIX = "uri|";
    private static final String[] TIME_BUCKETS =
            {"0.001", "0.01", "0.1", "1", "10", "100", "1000", "10000"};
    private static final String[] COUNT_BUCKETS =
            {"0", "9", "99", "999", "9999", "99999"};

    interface StatService {
        String service(String path);
    }

    private final DruidStatProperties.Prometheus config;
    private final ObjectProvider<MeterRegistry> meterRegistryProvider;
    private final StatService statService;
    private final ConcurrentMap<String, Series> series = new ConcurrentHashMap<String, Series>();

    private MeterRegistry meterRegistry;
    private ScheduledExecutorService scheduler;

    public DruidPrometheusMetricsExporter(DruidStatProperties.Prometheus config,
                                          MeterRegistry meterRegistry) {
        this(config, meterRegistry, null, new StatService() {
            @Override
            public String service(String path) {
                return DruidStatService.getInstance().service(path);
            }
        });
    }

    DruidPrometheusMetricsExporter(DruidStatProperties.Prometheus config,
                                   ObjectProvider<MeterRegistry> meterRegistryProvider) {
        this(config, null, meterRegistryProvider, new StatService() {
            @Override
            public String service(String path) {
                return DruidStatService.getInstance().service(path);
            }
        });
    }

    DruidPrometheusMetricsExporter(DruidStatProperties.Prometheus config,
                                   MeterRegistry meterRegistry,
                                   StatService statService) {
        this(config, meterRegistry, null, statService);
    }

    private DruidPrometheusMetricsExporter(DruidStatProperties.Prometheus config,
                                           MeterRegistry meterRegistry,
                                           ObjectProvider<MeterRegistry> meterRegistryProvider,
                                           StatService statService) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (meterRegistry == null && meterRegistryProvider == null) {
            throw new IllegalArgumentException("meterRegistry or meterRegistryProvider must not be null");
        }
        if (statService == null) {
            throw new IllegalArgumentException("statService must not be null");
        }
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.meterRegistryProvider = meterRegistryProvider;
        this.statService = statService;
    }

    @PostConstruct
    public synchronized void init() {
        if (scheduler != null) {
            return;
        }
        if (meterRegistry == null) {
            meterRegistry = meterRegistryProvider.getIfAvailable();
        }
        if (meterRegistry == null) {
            return;
        }
        refresh();
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "druid-micrometer-refresh");
                thread.setDaemon(true);
                return thread;
            }
        });
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refresh();
            }
        }, REFRESH_INTERVAL_SECONDS, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void destroy() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (meterRegistry != null) {
            for (Series value : series.values()) {
                meterRegistry.remove(value.meter);
            }
        }
        series.clear();
    }

    synchronized void refresh() {
        if (config.isWeburi()) {
            List<?> webUris = readList("/weburi.json");
            if (webUris != null) {
                updateWebUriMetrics(webUris);
            }
        }
        if (config.isSql()) {
            List<?> sqlStats = readList("/sql.json");
            if (sqlStats != null) {
                updateSqlMetrics(sqlStats);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void updateWebUriMetrics(List<?> webUris) {
        Set<String> seen = new HashSet<String>();
        for (Object item : webUris) {
            if (!(item instanceof Map)) {
                continue;
            }
            Map<String, Object> stat = (Map<String, Object>) item;
            String uri = string(stat.get("URI"));
            if (uri.isEmpty()) {
                continue;
            }
            Number requestCount = number(stat, "RequestCount");
            Number requestTime = number(stat, "RequestTimeMillis");
            Tags tags = Tags.of("uri", uri);
            dynamicUpdate(seen, URI_SERIES_PREFIX, "requestCount", "druid_uri_request_count_sum",
                    "Count of URI requests", tags, requestCount);
            dynamicUpdate(seen, URI_SERIES_PREFIX, "requestTime", "druid_uri_request_time_sum",
                    "Total URI request time in milliseconds", tags, requestTime);
            dynamicUpdate(seen, URI_SERIES_PREFIX, "requestMax", "druid_uri_request_time_max",
                    "Maximum URI request time in milliseconds", tags, number(stat, "RequestTimeMillisMax"));
            dynamicUpdate(seen, URI_SERIES_PREFIX, "requestAvg", "druid_uri_request_time_avg",
                    "Average URI request time in milliseconds", tags, average(requestTime, requestCount));
            updateHistogram(seen, URI_SERIES_PREFIX, "requestHistogram",
                    "druid_uri_request_time_histogram", "URI request time histogram",
                    tags, list(stat.get("Histogram")), TIME_BUCKETS);
            dynamicUpdate(seen, URI_SERIES_PREFIX, "jdbcExecute", "druid_uri_jdbc_execute_time_peak",
                    "Peak URI JDBC execute time", tags, number(stat, "JdbcExecutePeak"));
            dynamicUpdate(seen, URI_SERIES_PREFIX, "jdbcFetch", "druid_uri_jdbc_fetch_row_peak",
                    "Peak URI JDBC fetched rows", tags, number(stat, "JdbcFetchRowPeak"));
            dynamicUpdate(seen, URI_SERIES_PREFIX, "jdbcEffect", "druid_uri_jdbc_effect_row_peak",
                    "Peak URI JDBC affected rows", tags, number(stat, "JdbcUpdatePeak"));
        }
        removeMissing(URI_SERIES_PREFIX, seen);
    }

    @SuppressWarnings("unchecked")
    private void updateSqlMetrics(List<?> sqlStats) {
        Set<String> seen = new HashSet<String>();
        for (Object item : sqlStats) {
            if (!(item instanceof Map)) {
                continue;
            }
            Map<String, Object> stat = (Map<String, Object>) item;
            String sql = string(stat.get("SQL"));
            if (sql.trim().isEmpty()) {
                continue;
            }
            String hash = calculateSqlMd5(sql);
            Tags tags = Tags.of("sql", hash);
            Number executeCount = number(stat, "ExecuteCount");
            Number executeTime = number(stat, "ExecuteAndResultSetHoldTime");
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "executeCount", "druid_sql_execute_count_sum",
                    "Count of SQL executions", tags, executeCount);
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "executeTime", "druid_sql_execute_time_sum",
                    "Total SQL execution time in milliseconds", tags, executeTime);
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "executeMax", "druid_sql_execute_time_max",
                    "Maximum SQL execution time in milliseconds", tags, number(stat, "MaxTimespan"));
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "executeAvg", "druid_sql_execute_time_avg",
                    "Average SQL execution time in milliseconds", tags, average(executeTime, executeCount));
            updateHistogram(seen, SQL_SERIES_PREFIX, "executeHistogram",
                    "druid_sql_execute_time_histogram", "SQL execution time histogram",
                    tags, list(stat.get("ExecuteAndResultHoldTimeHistogram")), TIME_BUCKETS);
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "effectSum", "druid_sql_effect_row_sum",
                    "Total SQL affected rows", tags, number(stat, "EffectedRowCount"));
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "effectMax", "druid_sql_effect_row_max",
                    "Maximum SQL affected rows", tags, number(stat, "EffectedRowCountMax"));
            updateHistogram(seen, SQL_SERIES_PREFIX, "effectHistogram",
                    "druid_sql_effect_row_histogram", "SQL affected-row histogram",
                    tags, list(stat.get("EffectedRowCountHistogram")), COUNT_BUCKETS);
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "fetchSum", "druid_sql_fetch_row_sum",
                    "Total SQL fetched rows", tags, number(stat, "FetchRowCount"));
            dynamicUpdate(seen, SQL_SERIES_PREFIX, "fetchMax", "druid_sql_fetch_row_max",
                    "Maximum SQL fetched rows", tags, number(stat, "FetchRowCountMax"));
            updateHistogram(seen, SQL_SERIES_PREFIX, "fetchHistogram",
                    "druid_sql_fetch_row_histogram", "SQL fetched-row histogram",
                    tags, list(stat.get("FetchRowCountHistogram")), COUNT_BUCKETS);
        }
        removeMissing(SQL_SERIES_PREFIX, seen);
    }

    private void updateHistogram(Set<String> seen, String prefix, String identity,
                                 String name, String description, Tags baseTags,
                                 List<?> values, String[] buckets) {
        for (int i = 0; i < buckets.length; i++) {
            Number value = i < values.size() && values.get(i) instanceof Number
                    ? (Number) values.get(i) : Integer.valueOf(0);
            dynamicUpdate(seen, prefix, identity + "|" + buckets[i], name, description,
                    baseTags.and("max", buckets[i]), value);
        }
    }

    private void dynamicUpdate(Set<String> seen, String prefix, String identity,
                               String name, String description, Tags tags, Number value) {
        String key = prefix + identity + "|" + tags.toString();
        seen.add(key);
        update(key, name, description, tags, value);
    }

    private void update(String key, String name, String description, Tags tags, Number value) {
        Series current = series.get(key);
        if (current == null) {
            Meter existing = meterRegistry.find(name).tags(tags).meter();
            if (existing != null) {
                return;
            }
            AtomicReference<Double> reference = new AtomicReference<Double>(value.doubleValue());
            Gauge gauge = Gauge.builder(name, reference, new java.util.function.ToDoubleFunction<AtomicReference<Double>>() {
                @Override
                public double applyAsDouble(AtomicReference<Double> target) {
                    Double result = target.get();
                    return result == null ? 0 : result.doubleValue();
                }
            }).tags(tags).description(description).strongReference(true).register(meterRegistry);
            current = new Series(reference, gauge);
            series.put(key, current);
        }
        current.value.set(value.doubleValue());
    }

    private void removeMissing(String prefix, Set<String> seen) {
        for (Map.Entry<String, Series> entry : series.entrySet()) {
            if (entry.getKey().startsWith(prefix) && !seen.contains(entry.getKey())) {
                if (series.remove(entry.getKey(), entry.getValue())) {
                    meterRegistry.remove(entry.getValue().meter);
                }
            }
        }
    }

    private List<?> readList(String path) {
        String json;
        try {
            json = statService.service(path);
        } catch (RuntimeException e) {
            return null;
        }
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            Object parsed = JSONUtils.parse(json);
            if (!(parsed instanceof Map)) {
                return null;
            }
            Map<?, ?> result = (Map<?, ?>) parsed;
            Object resultCode = result.get("ResultCode");
            if (resultCode instanceof Number && ((Number) resultCode).intValue() != 1) {
                return null;
            }
            Object content = result.get("Content");
            if (content == null) {
                return Collections.emptyList();
            }
            return content instanceof List ? (List<?>) content : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Number number(Map<String, Object> values, String key) {
        Object value = values.get(key);
        return value instanceof Number ? (Number) value : Integer.valueOf(0);
    }

    private static Number average(Number sum, Number count) {
        double divisor = count == null ? 0 : count.doubleValue();
        if (divisor <= 0) {
            return Integer.valueOf(0);
        }
        double result = sum == null ? 0 : sum.doubleValue() / divisor;
        return Double.isFinite(result) ? Double.valueOf(result) : Integer.valueOf(0);
    }

    private static String string(Object value) {
        return value == null ? "" : value.toString();
    }

    private static List<?> list(Object value) {
        return value instanceof List ? (List<?>) value : Collections.emptyList();
    }

    static String calculateSqlMd5(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(sql.getBytes(StandardCharsets.UTF_8));
            StringBuilder hash = new StringBuilder(32);
            for (byte hashByte : hashBytes) {
                hash.append(String.format("%02x", hashByte & 0xff));
            }
            return hash.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 is not available", e);
        }
    }

    static String getTimeBucketLabel(int index) {
        return TIME_BUCKETS[index];
    }

    static String getCountBucketLabel(int index) {
        return COUNT_BUCKETS[index];
    }

    private static final class Series {
        private final AtomicReference<Double> value;
        private final Meter meter;

        private Series(AtomicReference<Double> value, Meter meter) {
            this.value = value;
            this.meter = meter;
        }
    }
}
