# Druid Prometheus Metrics Integration

This document describes how to use the Prometheus metrics integration in Druid Spring Boot Starter.

## Overview

The Druid Prometheus metrics integration allows you to expose Druid database connection pool statistics as Prometheus metrics. This enables monitoring and alerting on your database performance using Prometheus and Grafana.

## Configuration

### 1. Enable Prometheus Metrics

Add the following configuration to your `application.properties` or `application.yml`:

```properties
# Enable Prometheus metrics
spring.datasource.druid.prometheus.enabled=true

# Enable actuator
spring.datasource.druid.actuator.enabled=true
```

### 2. Configure Metrics Types

You can enable specific types of metrics:

```properties
# Enable all metrics types
spring.datasource.druid.actuator.basic=true
spring.datasource.druid.actuator.datasource=true
spring.datasource.druid.actuator.sql=true
spring.datasource.druid.actuator.weburi=true
spring.datasource.druid.actuator.websession=true
```

### 3. Actuator Configuration

Make sure Spring Boot Actuator is properly configured:

```properties
# Expose all actuator endpoints
management.endpoints.web.exposure.include=*

# Show details in health endpoint
management.endpoint.health.show-details=always
```

## Available Metrics

### Basic Metrics
- `druid_basic_active_connections`: Number of active connections
- `druid_basic_pool_connections`: Number of connections in the pool
- `druid_basic_pool_max_connections`: Maximum number of connections in the pool
- `druid_basic_execute_count`: Number of SQL executions (Counter)
- `druid_basic_error_count`: Number of SQL execution errors (Counter)
- `druid_basic_commit_count`: Number of transaction commits (Counter)
- `druid_basic_rollback_count`: Number of transaction rollbacks (Counter)
- `druid_basic_wait_thread_count`: Number of threads waiting for a connection
- `druid_basic_not_idle_connection_count`: Number of times that a connection was requested but no idle connection was available

### DataSource Metrics
- `druid_datasource_count`: Number of data sources
- `druid_datasource_pool_connections`: Number of connections in the pool (per datasource)
- `druid_datasource_active_connections`: Number of active connections (per datasource)

### SQL Metrics
- `druid_sql_execute_count_total`: Total number of SQL executions (Counter)
- `druid_sql_error_count_total`: Total number of SQL execution errors (Counter)
- `druid_sql_execute_time_total`: Total SQL execution time (Distribution Summary)
- `druid_sql_execute_count`: Number of SQL executions (per datasource, SQL type, SQL MD5)
- `druid_sql_execute_time`: SQL execution time (Timer)
- `druid_sql_duration`: SQL duration distribution (Distribution Summary)
- `druid_sql_error_count`: SQL execution errors (Counter)

### Web URI Metrics
- `druid_uri_request_count`: Total number of URI requests
- `druid_uri_request_time`: URI request time
- `druid_uri_request_time_histogram`: URI request time distribution

### Web Session Metrics
- `druid_websession_active_count`: Number of active web sessions
- `druid_websession_session_count`: Total number of web sessions

## Labels and Tags

### SQL Metrics Labels
- `datasource`: Datasource name
- `sql_md5`: MD5 hash of the SQL statement (for identification)
- `sql_type`: Type of SQL (select, insert, update, delete, etc.)

### DataSource Metrics Labels
- `datasource_name`: Datasource name
- `datasource_url`: Datasource URL

## Example Configuration

```properties
# Complete configuration example
spring:
  datasource:
    druid:
      # Basic datasource configuration
      url: jdbc:mysql://localhost:3306/mydb
      username: myuser
      password: mypassword
      driver-class-name: com.mysql.cj.jdbc.Driver

      # Pool configuration
      initial-size: 5
      min-idle: 5
      max-active: 20
      max-wait: 60000

      # Enable Prometheus metrics
      prometheus:
        enabled: true

      # Enable actuator and metrics
      actuator:
        enabled: true
        basic: true
        datasource: true
        sql: true
        weburi: true
        websession: true

      # Stat view servlet (optional)
      stat-view-servlet:
        enabled: true
        url-pattern: /druid/*
        allow: 127.0.0.1
        login-username: admin
        login-password: admin

      # Web stat filter (optional)
      web-stat-filter:
        enabled: true
        url-pattern: /*
        exclusions: *.js,*.gif,*.jpg,*.png,*.css,*.ico,/druid/*

# Actuator configuration
management:
  endpoints:
    web:
      exposure:
        include: *
  endpoint:
    health:
      show-details: always
```

## Prometheus Configuration

Add the following to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'druid'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: /actuator/prometheus
```

## Grafana Dashboard

You can use the following queries in Grafana to visualize the metrics:

### Connection Pool Metrics
```
# Active connections
druid_basic_active_connections

# Pool utilization
druid_basic_pool_connections / druid_basic_pool_max_connections

# SQL execution rate
rate(druid_basic_execute_count[5m])

# Error rate
rate(druid_basic_error_count[5m])
```

### SQL Performance Metrics
```
# Slow queries
topk(10, druid_sql_duration_sum{datasource="$datasource"} / druid_sql_duration_count{datasource="$datasource"})

# SQL type distribution
sum(rate(druid_sql_execute_count{sql_type="$type"}[5m])) by (sql_type)

# Error rate by SQL
topk(10, rate(druid_sql_error_count[5m])) by (sql_md5, datasource)
```

### Web Performance Metrics
```
# Request rate
rate(druid_uri_request_count[5m])

# Average response time
sum(rate(druid_uri_request_time_sum[5m])) / sum(rate(druid_uri_request_count[5m]))

# Slow URIs
topk(10, druid_uri_request_time_histogram_sum) by (uri)
```

## Troubleshooting

### 1. Metrics Not Appearing

Check the following:
- Ensure `spring.datasource.druid.prometheus.enabled=true` is set
- Verify that the actuator endpoint is accessible at `/actuator/prometheus`
- Check the logs for any errors during metric registration

### 2. Missing SQL MD5 Tags

If you don't see SQL MD5 tags, ensure that:
- SQL statements are being collected by the stat filter
- The SQL statements are not empty or null

### 3. High Cardinality Issues

If you encounter high cardinality issues with SQL MD5 tags:
- Consider using a sampling rate or filtering specific SQL types
- Use `sql_md5` label in Prometheus queries to aggregate by SQL statement

### 4. Performance Impact

The metrics collection has minimal performance impact. If you notice performance issues:
- Consider disabling some metrics types that you don't need
- Adjust the scrape interval in Prometheus

## Testing

You can test the metrics by running the example application:

```bash
# Run with Prometheus configuration
mvn spring-boot:run -Dspring-boot.run.properties=classpath:application-prometheus.properties

# Access the metrics endpoint
curl http://localhost:8080/actuator/prometheus
```

The response should contain all the Druid metrics in Prometheus format.