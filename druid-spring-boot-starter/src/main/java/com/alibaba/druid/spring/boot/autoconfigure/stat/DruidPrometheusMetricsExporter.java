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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Converts Druid's JSON statistics into the Prometheus text exposition format.
 *
 * <p>The SQL and URI metric names and labels intentionally match druid2prom.
 * A scrape reads every Druid JSON endpoint at most once.</p>
 *
 * @author druid
 */
public final class DruidPrometheusMetricsExporter {
    private static final String[] TIME_BUCKETS =
            {"0.001", "0.01", "0.1", "1", "10", "100", "1000", "10000"};
    private static final String[] COUNT_BUCKETS =
            {"0", "9", "99", "999", "9999", "99999"};

    interface StatService {
        String service(String path);
    }

    private final DruidStatProperties.Prometheus config;
    private final StatService statService;

    public DruidPrometheusMetricsExporter(DruidStatProperties.Prometheus config) {
        this(config, new StatService() {
            @Override
            public String service(String path) {
                return DruidStatService.getInstance().service(path);
            }
        });
    }

    DruidPrometheusMetricsExporter(DruidStatProperties.Prometheus config, StatService statService) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (statService == null) {
            throw new IllegalArgumentException("statService must not be null");
        }
        this.config = config;
        this.statService = statService;
    }

    public String scrape() {
        StringBuilder out = new StringBuilder(16384);

        if (config.isBasic() || config.isDatasource()) {
            List<?> dataSources = contentAsList(read("/datasource.json"));
            if (config.isBasic()) {
                appendBasicMetrics(out, dataSources);
            }
            if (config.isDatasource()) {
                appendDataSourceMetrics(out, dataSources);
            }
        }
        if (config.isWeburi()) {
            appendWebUriMetrics(out, contentAsList(read("/weburi.json")));
        }
        if (config.isSql()) {
            appendSqlMetrics(out, contentAsList(read("/sql.json")));
        }
        if (config.isWebsession()) {
            appendWebSessionMetrics(out, contentAsList(read("/websession.json")));
        }
        return out.toString();
    }

    private String read(String path) {
        try {
            return statService.service(path);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private void appendBasicMetrics(StringBuilder out, List<?> dataSources) {
        appendFamily(out, "druid_active_connections", "gauge", "Number of active connections");
        appendSample(out, "druid_active_connections", sum(dataSources, "ActiveCount"));
        appendFamily(out, "druid_pooling_connections", "gauge", "Number of connections in the pool");
        appendSample(out, "druid_pooling_connections", sum(dataSources, "PoolingCount"));
        appendFamily(out, "druid_pooling_max_connections", "gauge", "Maximum number of pooled connections");
        appendSample(out, "druid_pooling_max_connections", sum(dataSources, "MaxActive"));
        appendFamily(out, "druid_execute_count", "gauge", "Total number of SQL executions");
        appendSample(out, "druid_execute_count", sum(dataSources, "ExecuteCount"));
        appendFamily(out, "druid_error_count", "gauge", "Total number of SQL execution errors");
        appendSample(out, "druid_error_count", sum(dataSources, "ErrorCount"));
        appendFamily(out, "druid_commit_count", "gauge", "Total number of transaction commits");
        appendSample(out, "druid_commit_count", sum(dataSources, "CommitCount"));
        appendFamily(out, "druid_rollback_count", "gauge", "Total number of transaction rollbacks");
        appendSample(out, "druid_rollback_count", sum(dataSources, "RollbackCount"));
        appendFamily(out, "druid_wait_thread_count", "gauge", "Number of threads waiting for a connection");
        appendSample(out, "druid_wait_thread_count", sum(dataSources, "WaitThreadCount"));
        appendFamily(out, "druid_not_empty_wait_count", "gauge", "Number of non-empty connection waits");
        appendSample(out, "druid_not_empty_wait_count", sum(dataSources, "NotEmptyWaitCount"));
    }

    private void appendDataSourceMetrics(StringBuilder out, List<?> dataSources) {
        appendFamily(out, "druid_datasource_count", "gauge", "Number of Druid data sources");
        appendSample(out, "druid_datasource_count", Integer.valueOf(dataSources.size()));
        appendFamily(out, "druid_datasource_active_connections", "gauge",
                "Active connections across all data sources");
        appendSample(out, "druid_datasource_active_connections", sum(dataSources, "ActiveCount"));
        appendFamily(out, "druid_datasource_pooling_connections", "gauge",
                "Pooling connections across all data sources");
        appendSample(out, "druid_datasource_pooling_connections", sum(dataSources, "PoolingCount"));
    }

    @SuppressWarnings("unchecked")
    private void appendWebUriMetrics(StringBuilder out, List<?> webUris) {
        appendFamily(out, "druid_uri_request_count_sum", "counter", "Count of URI requests");
        appendFamily(out, "druid_uri_request_time_sum", "counter", "Total URI request time in milliseconds");
        appendFamily(out, "druid_uri_request_time_max", "gauge", "Maximum URI request time in milliseconds");
        appendFamily(out, "druid_uri_request_time_avg", "gauge", "Average URI request time in milliseconds");
        appendFamily(out, "druid_uri_request_time_histogram", "counter", "URI request time histogram");
        appendFamily(out, "druid_uri_jdbc_execute_time_peak", "gauge", "Peak URI JDBC execute time");
        appendFamily(out, "druid_uri_jdbc_fetch_row_peak", "gauge", "Peak URI JDBC fetched rows");
        appendFamily(out, "druid_uri_jdbc_effect_row_peak", "gauge", "Peak URI JDBC affected rows");

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
            appendSample(out, "druid_uri_request_count_sum", requestCount, "uri", uri);
            appendSample(out, "druid_uri_request_time_sum", requestTime, "uri", uri);
            appendSample(out, "druid_uri_request_time_max", number(stat, "RequestTimeMillisMax"), "uri", uri);
            appendSample(out, "druid_uri_request_time_avg", average(requestTime, requestCount), "uri", uri);
            appendHistogram(out, "druid_uri_request_time_histogram", list(stat.get("Histogram")),
                    TIME_BUCKETS, "uri", uri);
            appendSample(out, "druid_uri_jdbc_execute_time_peak", number(stat, "JdbcExecutePeak"), "uri", uri);
            appendSample(out, "druid_uri_jdbc_fetch_row_peak", number(stat, "JdbcFetchRowPeak"), "uri", uri);
            appendSample(out, "druid_uri_jdbc_effect_row_peak", number(stat, "JdbcUpdatePeak"), "uri", uri);
        }
    }

    @SuppressWarnings("unchecked")
    private void appendSqlMetrics(StringBuilder out, List<?> sqlStats) {
        appendFamily(out, "druid_sql_execute_count_sum", "counter", "Count of SQL executions");
        appendFamily(out, "druid_sql_execute_time_sum", "counter", "Total SQL execution time in milliseconds");
        appendFamily(out, "druid_sql_execute_time_max", "gauge", "Maximum SQL execution time in milliseconds");
        appendFamily(out, "druid_sql_execute_time_avg", "gauge", "Average SQL execution time in milliseconds");
        appendFamily(out, "druid_sql_execute_time_histogram", "counter", "SQL execution time histogram");
        appendFamily(out, "druid_sql_effect_row_sum", "counter", "Total SQL affected rows");
        appendFamily(out, "druid_sql_effect_row_max", "gauge", "Maximum SQL affected rows");
        appendFamily(out, "druid_sql_effect_row_histogram", "counter", "SQL affected-row histogram");
        appendFamily(out, "druid_sql_fetch_row_sum", "counter", "Total SQL fetched rows");
        appendFamily(out, "druid_sql_fetch_row_max", "gauge", "Maximum SQL fetched rows");
        appendFamily(out, "druid_sql_fetch_row_histogram", "counter", "SQL fetched-row histogram");

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
            Number executeCount = number(stat, "ExecuteCount");
            Number executeTime = number(stat, "ExecuteAndResultSetHoldTime");
            appendSample(out, "druid_sql_execute_count_sum", executeCount, "sql", hash);
            appendSample(out, "druid_sql_execute_time_sum", executeTime, "sql", hash);
            appendSample(out, "druid_sql_execute_time_max", number(stat, "MaxTimespan"), "sql", hash);
            appendSample(out, "druid_sql_execute_time_avg", average(executeTime, executeCount), "sql", hash);
            appendHistogram(out, "druid_sql_execute_time_histogram",
                    list(stat.get("ExecuteAndResultHoldTimeHistogram")), TIME_BUCKETS, "sql", hash);
            appendSample(out, "druid_sql_effect_row_sum", number(stat, "EffectedRowCount"), "sql", hash);
            appendSample(out, "druid_sql_effect_row_max", number(stat, "EffectedRowCountMax"), "sql", hash);
            appendHistogram(out, "druid_sql_effect_row_histogram",
                    list(stat.get("EffectedRowCountHistogram")), COUNT_BUCKETS, "sql", hash);
            appendSample(out, "druid_sql_fetch_row_sum", number(stat, "FetchRowCount"), "sql", hash);
            appendSample(out, "druid_sql_fetch_row_max", number(stat, "FetchRowCountMax"), "sql", hash);
            appendHistogram(out, "druid_sql_fetch_row_histogram",
                    list(stat.get("FetchRowCountHistogram")), COUNT_BUCKETS, "sql", hash);
        }
    }

    private void appendWebSessionMetrics(StringBuilder out, List<?> sessions) {
        appendFamily(out, "druid_websession_active_count", "gauge", "Number of active web sessions");
        appendSample(out, "druid_websession_active_count", Integer.valueOf(sessions.size()));
        appendFamily(out, "druid_websession_session_count", "gauge", "Number of web sessions");
        appendSample(out, "druid_websession_session_count", Integer.valueOf(sessions.size()));
    }

    private static void appendHistogram(StringBuilder out, String metric, List<?> values,
                                        String[] buckets, String labelName, String labelValue) {
        for (int i = 0; i < buckets.length; i++) {
            Number value = i < values.size() && values.get(i) instanceof Number
                    ? (Number) values.get(i) : Integer.valueOf(0);
            appendSample(out, metric, value, labelName, labelValue, "max", buckets[i]);
        }
    }

    private static void appendFamily(StringBuilder out, String name, String type, String help) {
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static void appendSample(StringBuilder out, String name, Number value, String... labels) {
        out.append(name);
        if (labels.length > 0) {
            out.append('{');
            for (int i = 0; i < labels.length; i += 2) {
                if (i > 0) {
                    out.append(',');
                }
                out.append(labels[i]).append("=\"").append(escapeLabel(labels[i + 1])).append('"');
            }
            out.append('}');
        }
        out.append(' ').append(value == null ? "0" : value.toString()).append('\n');
    }

    private static String escapeLabel(String value) {
        return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"");
    }

    private static Number average(Number sum, Number count) {
        double divisor = count == null ? 0 : count.doubleValue();
        if (divisor <= 0) {
            return Integer.valueOf(0);
        }
        double result = sum == null ? 0 : sum.doubleValue() / divisor;
        return Double.isFinite(result) ? Double.valueOf(result) : Integer.valueOf(0);
    }

    private static Number number(Map<String, Object> values, String key) {
        Object value = values.get(key);
        return value instanceof Number ? (Number) value : Integer.valueOf(0);
    }

    @SuppressWarnings("unchecked")
    private static Number sum(List<?> values, String key) {
        long total = 0;
        for (Object value : values) {
            if (value instanceof Map) {
                total += number((Map<String, Object>) value, key).longValue();
            }
        }
        return Long.valueOf(total);
    }

    private static String string(Object value) {
        return value == null ? "" : value.toString();
    }

    private static List<?> list(Object value) {
        return value instanceof List ? (List<?>) value : Collections.emptyList();
    }

    private static List<?> contentAsList(String json) {
        Object content = content(json);
        return content instanceof List ? (List<?>) content : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private static Object content(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            Object parsed = JSONUtils.parse(json);
            if (!(parsed instanceof Map)) {
                return null;
            }
            Map<String, Object> result = (Map<String, Object>) parsed;
            Object resultCode = result.get("ResultCode");
            if (resultCode instanceof Number && ((Number) resultCode).intValue() != 1) {
                return null;
            }
            return result.get("Content");
        } catch (RuntimeException e) {
            return null;
        }
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
}
