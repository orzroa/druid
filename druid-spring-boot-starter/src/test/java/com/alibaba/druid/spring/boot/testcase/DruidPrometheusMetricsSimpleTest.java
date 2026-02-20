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
package com.alibaba.druid.spring.boot.testcase;

import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import com.alibaba.druid.spring.boot.autoconfigure.stat.DruidPrometheusMetricsConfiguration;
import com.alibaba.druid.stat.DruidStatService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Simple test for Druid Prometheus Metrics Configuration
 *
 * @author druid
 */
public class DruidPrometheusMetricsSimpleTest {

    @Mock
    private DruidStatService druidStatService;

    private MeterRegistry meterRegistry;
    private DruidStatProperties properties;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        meterRegistry = new SimpleMeterRegistry();
        properties = new DruidStatProperties();

        // Enable actuator with all metrics
        properties.setActuator(new DruidStatProperties.Actuator());
        properties.getActuator().setEnabled(true);
        properties.getActuator().setBasic(true);
        properties.getActuator().setDatasource(true);
        properties.getActuator().setSql(true);
        properties.getActuator().setWeburi(true);
        properties.getActuator().setWebsession(true);
    }

    @Test
    void testBasicMetrics() {
        // Mock basic stat response
        Map<String, Object> basicContent = new HashMap<>();
        basicContent.put("ActiveCount", 5);
        basicContent.put("PoolingCount", 10);
        basicContent.put("PoolingMaxCount", 20);
        basicContent.put("ExecuteCount", 100);
        basicContent.put("ErrorCount", 2);
        basicContent.put("CommitCount", 50);
        basicContent.put("RollbackCount", 5);
        basicContent.put("WaitThreadCount", 1);
        basicContent.put("NotEmptyWaitCount", 3);

        Map<String, Object> basicResponse = new HashMap<>();
        basicResponse.put("Content", basicContent);

        when(druidStatService.getInstance()).thenReturn(druidStatService);
        when(druidStatService.service("/basic.json")).thenReturn("{ \"Content\": { \"ActiveCount\": 5, \"PoolingCount\": 10 } }");

        // Create configuration
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(properties, meterRegistry);

        // Verify basic metrics are registered
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_active_connections")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_pool_connections")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_pool_max_connections")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_execute_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_error_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_commit_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_rollback_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_wait_thread_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_basic_not_idle_connection_count")));

        // Test basic metric values
        Gauge activeConnections = (Gauge) meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("druid_basic_active_connections"))
                .findFirst().orElse(null);
        assertNotNull(activeConnections);
        assertEquals(5.0, activeConnections.value());

        Gauge poolConnections = (Gauge) meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("druid_basic_pool_connections"))
                .findFirst().orElse(null);
        assertNotNull(poolConnections);
        assertEquals(10.0, poolConnections.value());
    }

    @Test
    void testSqlMd5Calculation() {
        // Create configuration
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(properties, meterRegistry);

        String sql1 = "SELECT * FROM users";
        String sql2 = "SELECT * FROM users";  // Same SQL
        String sql3 = "INSERT INTO users (name, age) VALUES (?, ?)";  // Different SQL

        String md51 = config.calculateSqlMd5(sql1);
        String md52 = config.calculateSqlMd5(sql2);
        String md53 = config.calculateSqlMd5(sql3);

        assertEquals(md51, md52);  // Same SQL should have same MD5
        assertNotEquals(md51, md53);  // Different SQL should have different MD5
        assertFalse(md51.isEmpty());
        assertFalse(md53.isEmpty());
    }

    @Test
    void testSqlTypeDetection() {
        // Create configuration
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(properties, meterRegistry);

        assertEquals("select", config.determineSqlType("SELECT * FROM users"));
        assertEquals("insert", config.determineSqlType("INSERT INTO users VALUES (1, 'test')"));
        assertEquals("update", config.determineSqlType("UPDATE users SET name = 'test'"));
        assertEquals("delete", config.determineSqlType("DELETE FROM users WHERE id = 1"));
        assertEquals("create", config.determineSqlType("CREATE TABLE users (id INT)"));
        assertEquals("alter", config.determineSqlType("ALTER TABLE users ADD COLUMN age INT"));
        assertEquals("drop", config.determineSqlType("DROP TABLE users"));
        assertEquals("other", config.determineSqlType("EXECUTE PROCEDURE test()"));
        assertEquals("unknown", config.determineSqlType(""));
        assertEquals("unknown", config.determineSqlType(null));
    }

    @Test
    void testMetricsWithoutActuator() {
        // Test that metrics are not registered when actuator is disabled
        DruidStatProperties disabledActuatorProps = new DruidStatProperties();
        disabledActuatorProps.setActuator(new DruidStatProperties.Actuator());
        disabledActuatorProps.getActuator().setEnabled(false);

        SimpleMeterRegistry testRegistry = new SimpleMeterRegistry();
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(
                disabledActuatorProps, testRegistry);

        // No metrics should be registered when actuator is disabled
        assertEquals(0, testRegistry.getMeters().size());
    }

    @Test
    void testMetricsWithPartialActuator() {
        // Test that only enabled metrics are registered
        DruidStatProperties partialActuatorProps = new DruidStatProperties();
        DruidStatProperties.Actuator actuator = new DruidStatProperties.Actuator();
        actuator.setEnabled(true);
        actuator.setBasic(true);
        actuator.setDatasource(false);  // Disabled
        actuator.setSql(true);
        actuator.setWeburi(false);  // Disabled
        actuator.setWebsession(false);  // Disabled
        partialActuatorProps.setActuator(actuator);

        SimpleMeterRegistry testRegistry = new SimpleMeterRegistry();
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(
                partialActuatorProps, testRegistry);

        // Only basic and SQL metrics should be registered
        long basicCount = testRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().startsWith("druid_basic_"))
                .count();
        long datasourceCount = testRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().startsWith("druid_datasource_"))
                .count();
        long sqlCount = testRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().startsWith("druid_sql_"))
                .count();
        long uriCount = testRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().startsWith("druid_uri_"))
                .count();
        long sessionCount = testRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().startsWith("druid_websession_"))
                .count();

        assertTrue(basicCount > 0);
        assertEquals(0, datasourceCount);
        assertTrue(sqlCount > 0);
        assertEquals(0, uriCount);
        assertEquals(0, sessionCount);
    }

    @Test
    void testBuildMetricName() {
        // Create configuration
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(properties, meterRegistry);

        assertEquals("druid_test", config.buildMetricName("test"));
        assertEquals("druid_basic_connections", config.buildMetricName("basic_connections"));
        assertEquals("druid_sql_execute_time", config.buildMetricName("sql_execute_time"));
    }
}