# Druid Prometheus Metrics

The Druid Spring Boot Starter can expose Druid statistics directly in the
Prometheus text format. Spring Boot Actuator, Micrometer, and the standalone
`druid2prom` process are not required.

## Enable the endpoint

```properties
spring.datasource.druid.prometheus.enabled=true
```

The default scrape URL is:

```text
http://localhost:8080/actuator/prometheus
```

The URL and metric groups can be configured independently:

```properties
spring.datasource.druid.prometheus.url-pattern=/actuator/prometheus
spring.datasource.druid.prometheus.basic=true
spring.datasource.druid.prometheus.datasource=true
spring.datasource.druid.prometheus.sql=true
spring.datasource.druid.prometheus.weburi=true
spring.datasource.druid.prometheus.websession=true
```

Prometheus export is disabled by default. All metric groups default to enabled
after the endpoint itself is enabled.

SQL metrics require Druid's stat filter. URI and session metrics require the
web stat filter:

```properties
spring.datasource.druid.filter.stat.enabled=true
spring.datasource.druid.web-stat-filter.enabled=true
spring.datasource.druid.web-stat-filter.exclusions=*.js,*.gif,*.jpg,*.png,*.css,*.ico,/druid/*,/actuator/prometheus
```

When a custom scrape URL is used, add that URL to the web-stat exclusions to
avoid counting Prometheus scrapes as application requests.

## druid2prom-compatible metrics

The embedded exporter preserves the 19 metric families, names, labels, MD5
calculation, and bucket labels used by `druid2prom`.

| Metric | Type | Labels |
| --- | --- | --- |
| `druid_uri_request_count_sum` | counter | `uri` |
| `druid_uri_request_time_sum` | counter | `uri` |
| `druid_uri_request_time_max` | gauge | `uri` |
| `druid_uri_request_time_avg` | gauge | `uri` |
| `druid_uri_request_time_histogram` | counter | `uri`, `max` |
| `druid_uri_jdbc_execute_time_peak` | gauge | `uri` |
| `druid_uri_jdbc_fetch_row_peak` | gauge | `uri` |
| `druid_uri_jdbc_effect_row_peak` | gauge | `uri` |
| `druid_sql_execute_count_sum` | counter | `sql` |
| `druid_sql_execute_time_sum` | counter | `sql` |
| `druid_sql_execute_time_max` | gauge | `sql` |
| `druid_sql_execute_time_avg` | gauge | `sql` |
| `druid_sql_execute_time_histogram` | counter | `sql`, `max` |
| `druid_sql_effect_row_sum` | counter | `sql` |
| `druid_sql_effect_row_max` | gauge | `sql` |
| `druid_sql_effect_row_histogram` | counter | `sql`, `max` |
| `druid_sql_fetch_row_sum` | counter | `sql` |
| `druid_sql_fetch_row_max` | gauge | `sql` |
| `druid_sql_fetch_row_histogram` | counter | `sql`, `max` |

The `sql` label is the lowercase MD5 of the UTF-8 SQL text. The original SQL
remains available from Druid's SQL statistics page and `/druid/sql.json`.

The exporter also provides optional basic, datasource, and web-session gauges:

- `druid_active_connections`
- `druid_pooling_connections`
- `druid_pooling_max_connections`
- `druid_execute_count`
- `druid_error_count`
- `druid_commit_count`
- `druid_rollback_count`
- `druid_wait_thread_count`
- `druid_not_empty_wait_count`
- `druid_datasource_count`
- `druid_datasource_active_connections`
- `druid_datasource_pooling_connections`
- `druid_websession_active_count`
- `druid_websession_session_count`

## Prometheus configuration

```yaml
scrape_configs:
  - job_name: druid
    static_configs:
      - targets: ["localhost:8080"]
    metrics_path: /actuator/prometheus
```

The scrape endpoint intentionally contains no raw SQL, datasource URL,
credentials, or stack traces. URI values are escaped according to the
Prometheus text format. As with any monitoring endpoint, restrict access at the
network or application security layer when the service is not on a trusted
network.

Unlike the original standalone exporter, scraping does not call
`/druid/reset-all.json`. Resetting cumulative values during collection would
make Prometheus counters discontinuous and would mutate application monitoring
state.

## Quick check

```bash
curl http://localhost:8080/actuator/prometheus
```

Each scrape reads each enabled Druid JSON endpoint no more than once, so the
work is linear in the number of SQL and URI statistic entries.
