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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.spring.boot.autoconfigure.stat;

import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import com.alibaba.druid.stat.DruidStatService;
import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.druid.support.logging.Log;
import com.alibaba.druid.support.logging.LogFactory;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Druid Prometheus Actuator Configuration.
 * Expose Druid statistics as Prometheus metrics via HTTP endpoints.
 *
 * @author druid
 */
@Configuration
@ConditionalOnWebApplication
@ConditionalOnClass({MeterRegistry.class, MeterBinder.class})
@ConditionalOnProperty(name = "spring.datasource.druid.prometheus.enabled", havingValue = "true", matchIfMissing = true)
public class DruidPrometheusMetricsConfiguration {

    private static final Log LOG = LogFactory.getLog(DruidPrometheusMetricsConfiguration.class);
    private static final String NAMESPACE = "druid";

    private final DruidStatProperties properties;
    private final MeterRegistry meterRegistry;
    private final DruidStatService druidStatService;

    // 用于缓存已注册的指标，避免重复注册
    private final Set<String> registeredMetrics = new ConcurrentHashMap<>().newKeySet();

    public DruidPrometheusMetricsConfiguration(DruidStatProperties properties,
                                               MeterRegistry meterRegistry) {
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.druidStatService = DruidStatService.getInstance();
    }

    @PostConstruct
    public void init() {
        if (!properties.getActuator().isEnabled()) {
            return;
        }

        if (properties.getActuator().isBasic()) {
            registerBasicMetrics();
        }
        if (properties.getActuator().isDatasource()) {
            registerDataSourceMetrics();
        }
        if (properties.getActuator().isSql()) {
            registerSqlMetrics();
        }
        if (properties.getActuator().isWeburi()) {
            registerWebUriMetrics();
        }
        if (properties.getActuator().isWebsession()) {
            registerWebSessionMetrics();
        }
    }

    /**
     * 计算SQL的MD5值
     */
    private String calculateSqlMd5(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(sql.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("Failed to calculate SQL MD5", e);
            return "";
        }
    }

    /**
     * 构建指标名称
     */
    private String buildMetricName(String name) {
        return NAMESPACE + "_" + name;
    }

    /**
     * 检查指标是否已注册，避免重复注册
     */
    private boolean isMetricRegistered(String name) {
        return registeredMetrics.contains(name);
    }

    /**
     * 注册指标并标记为已注册
     */
    private void registerMetric(Meter meter) {
        String name = meter.getId().getName();
        if (!isMetricRegistered(name)) {
            meterRegistry.register(meter);
            registeredMetrics.add(name);
        }
    }

    private double getBasicStat(String key) {
        try {
            String result = druidStatService.service("/basic.json");
            Map<String, Object> data = parseResult(result);
            if (data != null) {
                Map<String, Object> content = (Map<String, Object>) data.get("Content");
                if (content != null && content.get(key) != null) {
                    return ((Number) content.get(key)).doubleValue();
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return 0.0;
    }

    private void registerBasicMetrics() {
        // Active connections
        registerMetric(Gauge.builder(buildMetricName("basic_active_connections"),
                druidStatService, service -> getBasicStat("ActiveCount"))
                .description("Number of active connections")
                .register(meterRegistry));

        // Pool connections
        registerMetric(Gauge.builder(buildMetricName("basic_pool_connections"),
                druidStatService, service -> getBasicStat("PoolingCount"))
                .description("Number of connections in the pool")
                .register(meterRegistry));

        // Pool max connections
        registerMetric(Gauge.builder(buildMetricName("basic_pool_max_connections"),
                druidStatService, service -> getBasicStat("PoolingMaxCount"))
                .description("Maximum number of connections in the pool")
                .register(meterRegistry));

        // Execute count (使用 Counter)
        registerMetric(Counter.builder(buildMetricName("basic_execute_count"))
                .description("Number of SQL executions")
                .register(meterRegistry));

        // Error count
        registerMetric(Counter.builder(buildMetricName("basic_error_count"))
                .description("Number of SQL execution errors")
                .register(meterRegistry));

        // Commit count
        registerMetric(Counter.builder(buildMetricName("basic_commit_count"))
                .description("Number of transaction commits")
                .register(meterRegistry));

        // Rollback count
        registerMetric(Counter.builder(buildMetricName("basic_rollback_count"))
                .description("Number of transaction rollbacks")
                .register(meterRegistry));

        // Wait thread count
        registerMetric(Gauge.builder(buildMetricName("basic_wait_thread_count"),
                druidStatService, service -> getBasicStat("WaitThreadCount"))
                .description("Number of threads waiting for a connection")
                .register(meterRegistry));

        // Not idle connection count
        registerMetric(Gauge.builder(buildMetricName("basic_not_idle_connection_count"),
                druidStatService, service -> getBasicStat("NotEmptyWaitCount"))
                .description("Number of times that a connection was requested but no idle connection was available")
                .register(meterRegistry));
    }

    private void registerDataSourceMetrics() {
        // Number of data sources
        registerMetric(Gauge.builder(buildMetricName("datasource_count"), druidStatService, service -> {
            try {
                String result = druidStatService.service("/datasource.json");
                Map<String, Object> data = parseResult(result);
                if (data != null) {
                    List<?> content = (List<?>) data.get("Content");
                    if (content != null) {
                        return (double) content.size();
                    }
                }
            } catch (Exception e) {
                // ignore
            }
            return 0.0;
        }).description("Number of data sources").register(meterRegistry));

        // 为每个数据源注册独立的指标
        try {
            String result = druidStatService.service("/datasource.json");
            Map<String, Object> data = parseResult(result);
            if (data != null) {
                List<?> content = (List<?>) data.get("Content");
                if (content != null) {
                    for (Object dsObj : content) {
                        Map<String, Object> ds = (Map<String, Object>) dsObj;
                        String dsName = ds.get("Name") != null ? ds.get("Name").toString() : "default";
                        String dsUrl = ds.get("Url") != null ? ds.get("Url").toString() : "";

                        // 数据源连接指标
                        registerMetric(Gauge.builder(buildMetricName("datasource_pool_connections"),
                                        ds, service -> {
                                    try {
                                        Object datasource = getDruidDataSourceByName(dsName);
                                        if (datasource != null) {
                                            Method method = datasource.getClass().getMethod("getPoolingCount");
                                            return ((Number) method.invoke(datasource)).doubleValue();
                                        }
                                    } catch (Exception e) {
                                        // ignore
                                    }
                                    return 0.0;
                                })
                                .tag("datasource_name", dsName)
                                .tag("datasource_url", dsUrl)
                                .description("Number of connections in the pool")
                                .register(meterRegistry));

                        // 活动连接数
                        registerMetric(Gauge.builder(buildMetricName("datasource_active_connections"),
                                        ds, service -> {
                                    try {
                                        Object datasource = getDruidDataSourceByName(dsName);
                                        if (datasource != null) {
                                            Method method = datasource.getClass().getMethod("getActiveCount");
                                            return ((Number) method.invoke(datasource)).doubleValue();
                                        }
                                    } catch (Exception e) {
                                        // ignore
                                    }
                                    return 0.0;
                                })
                                .tag("datasource_name", dsName)
                                .tag("datasource_url", dsUrl)
                                .description("Number of active connections")
                                .register(meterRegistry));
                    }
                }
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private Object getDruidDataSourceByName(String name) {
        try {
            Method method = DruidStatService.class.getMethod("getInstance");
            DruidStatService service = (DruidStatService) method.invoke(null);
            Method getMethod = DruidStatService.class.getMethod("getDruidDataSourceByName", String.class);
            return getMethod.invoke(service, name);
        } catch (Exception e) {
            return null;
        }
    }

    private void registerSqlMetrics() {
        // 注册汇总的SQL指标
        registerSqlSummaryMetrics();

        // 注册按数据源和SQL分组的详细指标
        registerSqlDetailedMetrics();
    }

    private void registerSqlSummaryMetrics() {
        // SQL execute count (汇总)
        registerMetric(Counter.builder(buildMetricName("sql_execute_count_total"))
                .description("Total number of SQL executions")
                .register(meterRegistry));

        // SQL error count (汇总)
        registerMetric(Counter.builder(buildMetricName("sql_error_count_total"))
                .description("Total number of SQL execution errors")
                .register(meterRegistry));

        // SQL execute time (汇总)
        registerMetric(DistributionSummary.builder(buildMetricName("sql_execute_time_total"))
                .description("Total SQL execution time in milliseconds")
                .register(meterRegistry));
    }

    private void registerSqlDetailedMetrics() {
        try {
            // 获取所有数据源的SQL统计
            String result = druidStatService.service("/sql.json");
            Map<String, Object> data = parseResult(result);
            if (data != null) {
                List<?> content = (List<?>) data.get("Content");
                if (content != null) {
                    for (Object sqlObj : content) {
                        Map<String, Object> sqlStat = (Map<String, Object>) sqlObj;
                        String sql = sqlStat.get("SQL") != null ? sqlStat.get("SQL").toString() : "";
                        String sqlMd5 = calculateSqlMd5(sql);
                        String sqlType = determineSqlType(sql);

                        // 获取数据源名称
                        String dataSourceName = sqlStat.get("DataSourceName") != null ?
                                sqlStat.get("DataSourceName").toString() : "default";

                        // SQL执行次数计数器
                        String executeCountMetric = buildMetricName("sql_execute_count");
                        if (!isMetricRegistered(executeCountMetric + "_" + sqlMd5)) {
                            registerMetric(Counter.builder(executeCountMetric)
                                    .tag("datasource", dataSourceName)
                                    .tag("sql_md5", sqlMd5)
                                    .tag("sql_type", sqlType)
                                    .description("Number of SQL executions")
                                    .register(meterRegistry));
                        }

                        // SQL执行时间计时器
                        String executeTimeMetric = buildMetricName("sql_execute_time");
                        if (!isMetricRegistered(executeTimeMetric + "_" + sqlMd5)) {
                            registerMetric(Timer.builder(executeTimeMetric)
                                    .tag("datasource", dataSourceName)
                                    .tag("sql_md5", sqlMd5)
                                    .tag("sql_type", sqlType)
                                    .description("SQL execution time")
                                    .register(meterRegistry));
                        }

                        // SQL执行时间直方图
                        String durationMetric = buildMetricName("sql_duration");
                        if (!isMetricRegistered(durationMetric + "_" + sqlMd5)) {
                            registerMetric(DistributionSummary.builder(durationMetric)
                                    .tag("datasource", dataSourceName)
                                    .tag("sql_md5", sqlMd5)
                                    .tag("sql_type", sqlType)
                                    .description("SQL duration distribution")
                                    .register(meterRegistry));
                        }

                        // SQL错误计数器
                        String errorCountMetric = buildMetricName("sql_error_count");
                        if (sqlStat.get("ErrorCount") != null) {
                            double errorCount = ((Number) sqlStat.get("ErrorCount")).doubleValue();
                            if (errorCount > 0 && !isMetricRegistered(errorCountMetric + "_" + sqlMd5)) {
                                registerMetric(Counter.builder(errorCountMetric)
                                        .tag("datasource", dataSourceName)
                                        .tag("sql_md5", sqlMd5)
                                        .tag("sql_type", sqlType)
                                        .description("Number of SQL execution errors")
                                        .register(meterRegistry));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to register SQL metrics", e);
        }
    }

    private String determineSqlType(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "unknown";
        }
        String upperSql = sql.trim().toUpperCase();
        if (upperSql.startsWith("SELECT")) {
            return "select";
        } else if (upperSql.startsWith("INSERT")) {
            return "insert";
        } else if (upperSql.startsWith("UPDATE")) {
            return "update";
        } else if (upperSql.startsWith("DELETE")) {
            return "delete";
        } else if (upperSql.startsWith("CREATE")) {
            return "create";
        } else if (upperSql.startsWith("ALTER")) {
            return "alter";
        } else if (upperSql.startsWith("DROP")) {
            return "drop";
        } else {
            return "other";
        }
    }

    private void registerWebUriMetrics() {
        // URI request count
        registerMetric(Gauge.builder(buildMetricName("uri_request_count"), druidStatService, service -> {
            try {
                String result = druidStatService.service("/weburi.json");
                Map<String, Object> data = parseResult(result);
                if (data != null) {
                    List<?> content = (List<?>) data.get("Content");
                    if (content != null) {
                        double total = 0;
                        for (Object item : content) {
                            Map<String, Object> uri = (Map<String, Object>) item;
                            if (uri.get("RequestCount") != null) {
                                total += ((Number) uri.get("RequestCount")).doubleValue();
                            }
                        }
                        return total;
                    }
                }
            } catch (Exception e) {
                // ignore
            }
            return 0.0;
        }).description("Total number of URI requests").register(meterRegistry));

        // URI request time
        registerMetric(DistributionSummary.builder(buildMetricName("uri_request_time"))
                .description("URI request time in milliseconds")
                .register(meterRegistry));

        // URI request time histogram
        registerMetric(DistributionSummary.builder(buildMetricName("uri_request_time_histogram"))
                .description("URI request time distribution")
                .register(meterRegistry));
    }

    private void registerWebSessionMetrics() {
        // Websession active count
        registerMetric(Gauge.builder(buildMetricName("websession_active_count"), druidStatService, service -> {
            try {
                String result = druidStatService.service("/websession.json");
                Map<String, Object> data = parseResult(result);
                if (data != null) {
                    List<?> content = (List<?>) data.get("Content");
                    if (content != null && !content.isEmpty()) {
                        Map<String, Object> session = (Map<String, Object>) content.get(0);
                        if (session.get("ActiveCount") != null) {
                            return ((Number) session.get("ActiveCount")).doubleValue();
                        }
                    }
                }
            } catch (Exception e) {
                // ignore
            }
            return 0.0;
        }).description("Number of active web sessions").register(meterRegistry));

        // Websession session count
        registerMetric(Gauge.builder(buildMetricName("websession_session_count"), druidStatService, service -> {
            try {
                String result = druidStatService.service("/websession.json");
                Map<String, Object> data = parseResult(result);
                if (data != null) {
                    List<?> content = (List<?>) data.get("Content");
                    if (content != null) {
                        return (double) content.size();
                    }
                }
            } catch (Exception e) {
                // ignore
            }
            return 0.0;
        }).description("Total number of web sessions").register(meterRegistry));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseResult(String result) {
        if (result == null || result.isEmpty()) {
            return null;
        }
        try {
            return (Map<String, Object>) JSONUtils.parse(result);
        } catch (Exception e) {
            return null;
        }
    }
}
