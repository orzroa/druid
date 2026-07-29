# Druid Prometheus Metrics

The Druid Spring Boot Starter can register Druid statistics in the
application's existing Micrometer `MeterRegistry`. It does not register a
Servlet or create another HTTP endpoint. The application's existing Actuator
Prometheus endpoint exposes both business metrics and Druid metrics.

## Enable Druid metric registration

The application must already provide Micrometer, a Prometheus registry, and
the Actuator Prometheus endpoint. For example, a Spring Boot 2 application
normally includes:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

Enable Druid registration and expose the application's Prometheus endpoint:

```properties
spring.datasource.druid.prometheus.enabled=true
management.endpoints.web.exposure.include=prometheus
```

Metric groups can be configured independently:

```properties
spring.datasource.druid.prometheus.basic=true
spring.datasource.druid.prometheus.datasource=true
spring.datasource.druid.prometheus.sql=true
spring.datasource.druid.prometheus.weburi=true
spring.datasource.druid.prometheus.websession=true
```

Druid metric registration is disabled by default. All metric groups default to
enabled after registration itself is enabled. If the application has no
Micrometer `MeterRegistry`, no Druid metrics or refresh task are started.

SQL metrics require Druid's stat filter. URI and session metrics require the
web stat filter:

```properties
spring.datasource.druid.filter.stat.enabled=true
spring.datasource.druid.web-stat-filter.enabled=true
spring.datasource.druid.web-stat-filter.exclusions=*.js,*.gif,*.jpg,*.png,*.css,*.ico,/druid/*,/actuator/prometheus
```

If Actuator uses a different path, add that path to the web-stat exclusions to
avoid counting Prometheus scrapes as application requests.

## druid2prom-compatible metrics

The registry exporter preserves the 19 metric families, names, labels, MD5
calculation, values, and bucket labels used by `druid2prom`.

| Metric | Type | Labels |
| --- | --- | --- |
| `druid_uri_request_count_sum` | gauge | `uri` |
| `druid_uri_request_time_sum` | gauge | `uri` |
| `druid_uri_request_time_max` | gauge | `uri` |
| `druid_uri_request_time_avg` | gauge | `uri` |
| `druid_uri_request_time_histogram` | gauge | `uri`, `max` |
| `druid_uri_jdbc_execute_time_peak` | gauge | `uri` |
| `druid_uri_jdbc_fetch_row_peak` | gauge | `uri` |
| `druid_uri_jdbc_effect_row_peak` | gauge | `uri` |
| `druid_sql_execute_count_sum` | gauge | `sql` |
| `druid_sql_execute_time_sum` | gauge | `sql` |
| `druid_sql_execute_time_max` | gauge | `sql` |
| `druid_sql_execute_time_avg` | gauge | `sql` |
| `druid_sql_execute_time_histogram` | gauge | `sql`, `max` |
| `druid_sql_effect_row_sum` | gauge | `sql` |
| `druid_sql_effect_row_max` | gauge | `sql` |
| `druid_sql_effect_row_histogram` | gauge | `sql`, `max` |
| `druid_sql_fetch_row_sum` | gauge | `sql` |
| `druid_sql_fetch_row_max` | gauge | `sql` |
| `druid_sql_fetch_row_histogram` | gauge | `sql`, `max` |

Druid reports absolute snapshot values which can be reset. They are registered
as gauges so Micrometer does not append `_total`, reject a decrease, or retain
a stale cumulative value. This preserves the `druid2prom` metric names and
observed values even though `druid2prom` described cumulative fields as
counters.

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

The Druid metrics contain no raw SQL, datasource URL, credentials, or stack
traces. SQL is represented by its MD5 label. Actuator and the Prometheus
registry perform text-format escaping and endpoint security.

Unlike the original standalone exporter, scraping does not call
`/druid/reset-all.json`. Resetting cumulative values during collection would
make Prometheus counters discontinuous and would mutate application monitoring
state.

## Quick check

```bash
curl http://localhost:8080/actuator/prometheus
```

Druid refreshes the in-memory gauges every 15 seconds and reads each enabled
JSON endpoint no more than once per refresh. A Prometheus scrape only reads
Micrometer gauges and does not call Druid's JSON endpoints.
