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

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceAutoConfigure;
import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import com.alibaba.druid.spring.boot.autoconfigure.stat.DruidPrometheusMetricsConfiguration;
import com.alibaba.druid.stat.DruidStatService;
import com.alibaba.druid.support.json.JSONUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test for Druid Prometheus Metrics Configuration
 *
 * @author druid
 */
@SpringBootTest(classes = DruidPrometheusMetricsTest.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:h2:mem:testdb",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.druid.initial-size=1",
        "spring.datasource.druid.min-idle=1",
        "spring.datasource.druid.max-active=10",
        "spring.datasource.druid.prometheus.enabled=true",
        "spring.datasource.druid.actuator.enabled=true",
        "spring.datasource.druid.actuator.basic=true",
        "spring.datasource.druid.actuator.datasource=true",
        "spring.datasource.druid.actuator.sql=true",
        "spring.datasource.druid.actuator.weburi=true",
        "spring.datasource.druid.actuator.websession=true"
})
public class DruidPrometheusMetricsTest {

    @MockBean
    private DruidStatService druidStatService;

    @MockBean
    private WebServerApplicationContext webServerApplicationContext;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private DruidStatProperties druidStatProperties;

    @Autowired
    private TestRestTemplate restTemplate;

    @TestConfiguration
    @Import({DruidDataSourceAutoConfigure.class, TestConfig.class})
    static class TestConfig {
        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        public DruidPrometheusMetricsConfiguration druidPrometheusMetricsConfiguration(
                DruidStatProperties properties, MeterRegistry meterRegistry) {
            return new DruidPrometheusMetricsConfiguration(properties, meterRegistry);
        }
    }

    @BeforeEach
    void setUp() {
        // Reset mocks
        reset(druidStatService);

        // Configure mock responses for DruidStatService
        when(druidStatService.getInstance()).thenReturn(druidStatService);

        // Mock basic.json response
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

        when(druidStatService.service("/basic.json")).thenReturn(JSONUtils.toJSONString(basicResponse));

        // Mock datasource.json response
        Map<String, Object> ds1 = new HashMap<>();
        ds1.put("Name", "primaryDataSource");
        ds1.put("Url", "jdbc:h2:mem:primary");

        Map<String, Object> ds2 = new HashMap<>();
        ds2.put("Name", "secondaryDataSource");
        ds2.put("Url", "jdbc:h2:mem:secondary");

        List<Map<String, Object>> datasourceContent = Arrays.asList(ds1, ds2);

        Map<String, Object> datasourceResponse = new HashMap<>();
        datasourceResponse.put("Content", datasourceContent);

        when(druidStatService.service("/datasource.json")).thenReturn(JSONUtils.toJSONString(datasourceResponse));

        // Mock sql.json response
        Map<String, Object> sql1 = new HashMap<>();
        sql1.put("SQL", "SELECT * FROM users");
        sql1.put("DataSourceName", "primaryDataSource");
        sql1.put("ExecuteCount", 10);
        sql1.put("ExecuteAndResultSetHoldTime", 100);
        sql1.put("MaxTimespan", 50);
        sql1.put("EffectedRowCount", 20);
        sql1.put("FetchRowCount", 30);
        sql1.put("ErrorCount", 0);

        Map<String, Object> sql2 = new HashMap<>();
        sql2.put("SQL", "INSERT INTO users (name, age) VALUES (?, ?)");
        sql2.put("DataSourceName", "primaryDataSource");
        sql2.put("ExecuteCount", 5);
        sql2.put("ExecuteAndResultSetHoldTime", 25);
        sql2.put("MaxTimespan", 10);
        sql2.put("EffectedRowCount", 5);
        sql2.put("FetchRowCount", 0);
        sql2.put("ErrorCount", 1);

        List<Map<String, Object>> sqlContent = Arrays.asList(sql1, sql2);

        Map<String, Object> sqlResponse = new HashMap<>();
        sqlResponse.put("Content", sqlContent);

        when(druidStatService.service("/sql.json")).thenReturn(JSONUtils.toJSONString(sqlResponse));

        // Mock weburi.json response
        Map<String, Object> uri1 = new HashMap<>();
        uri1.put("URI", "/api/users");
        uri1.put("RequestCount", 100);
        uri1.put("RequestTimeMillis", 1000);
        uri1.put("RequestTimeMillisMax", 100);
        uri1.put("JdbcExecutePeak", 10);
        uri1.put("JdbcFetchRowPeak", 20);
        uri1.put("JdbcUpdatePeak", 5);

        Map<String, Object> uri2 = new HashMap<>();
        uri2.put("URI", "/api/orders");
        uri2.put("RequestCount", 50);
        uri2.put("RequestTimeMillis", 500);
        uri2.put("RequestTimeMillisMax", 50);
        uri2.put("JdbcExecutePeak", 5);
        uri2.put("JdbcFetchRowPeak", 10);
        uri2.put("JdbcUpdatePeak", 2);

        List<Map<String, Object>> uriContent = Arrays.asList(uri1, uri2);

        Map<String, Object> uriResponse = new HashMap<>();
        uriResponse.put("Content", uriContent);

        when(druidStatService.service("/weburi.json")).thenReturn(JSONUtils.toJSONString(uriResponse));

        // Mock websession.json response
        Map<String, Object> session1 = new HashMap<>();
        session1.put("ActiveCount", 25);
        session1.put("SessionCount", 100);

        List<Map<String, Object>> sessionContent = Arrays.asList(session1);

        Map<String, Object> sessionResponse = new HashMap<>();
        sessionResponse.put("Content", sessionContent);

        when(druidStatService.service("/websession.json")).thenReturn(JSONUtils.toJSONString(sessionResponse));
    }

    @Test
    void testBasicMetricsRegistration() {
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
    }

    @Test
    void testBasicMetricsValues() {
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

        Gauge poolMaxConnections = (Gauge) meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("druid_basic_pool_max_connections"))
                .findFirst().orElse(null);
        assertNotNull(poolMaxConnections);
        assertEquals(20.0, poolMaxConnections.value());
    }

    @Test
    void testDataSourceMetricsRegistration() {
        // Verify datasource metrics are registered
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_datasource_count")));

        // Verify per-datasource metrics are registered
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_datasource_pool_connections")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_datasource_active_connections")));
    }

    @Test
    void testDataSourceMetricsValues() {
        // Test datasource count
        Gauge datasourceCount = (Gauge) meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("druid_datasource_count"))
                .findFirst().orElse(null);
        assertNotNull(datasourceCount);
        assertEquals(2.0, datasourceCount.value());
    }

    @Test
    void testSqlMetricsRegistration() {
        // Verify SQL summary metrics are registered
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_sql_execute_count_total")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_sql_error_count_total")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_sql_execute_time_total")));

        // Verify detailed SQL metrics are registered
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_sql_execute_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_sql_execute_time")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_sql_duration")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_sql_error_count")));
    }

    @Test
    void testSqlMd5Calculation() {
        // Test MD5 calculation for SQL
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(
                druidStatProperties, meterRegistry);

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
        // Test SQL type detection
        DruidPrometheusMetricsConfiguration config = new DruidPrometheusMetricsConfiguration(
                druidStatProperties, meterRegistry);

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
    void testWebUriMetricsRegistration() {
        // Verify web URI metrics are registered
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_uri_request_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_uri_request_time")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_uri_request_time_histogram")));
    }

    @Test
    void testWebUriMetricsValues() {
        // Test web URI metrics
        Gauge uriRequestCount = (Gauge) meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("druid_uri_request_count"))
                .findFirst().orElse(null);
        assertNotNull(uriRequestCount);
        assertEquals(150.0, uriRequestCount.value());
    }

    @Test
    void testWebSessionMetricsRegistration() {
        // Verify web session metrics are registered
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_websession_active_count")));
        assertTrue(meterRegistry.getMeters().stream()
                .anyMatch(m -> m.getId().getName().equals("druid_websession_session_count")));
    }

    @Test
    void testWebSessionMetricsValues() {
        // Test web session metrics
        Gauge sessionActiveCount = (Gauge) meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("druid_websession_active_count"))
                .findFirst().orElse(null);
        assertNotNull(sessionActiveCount);
        assertEquals(25.0, sessionActiveCount.value());

        Gauge sessionCount = (Gauge) meterRegistry.getMeters().stream()
                .filter(m -> m.getId().getName().equals("druid_websession_session_count"))
                .findFirst().orElse(null);
        assertNotNull(sessionCount);
        assertEquals(1.0, sessionCount.value());
    }

    @Test
    void testActuatorConfiguration() {
        // Verify actuator configuration
        assertNotNull(druidStatProperties);
        assertNotNull(druidStatProperties.getActuator());

        assertTrue(druidStatProperties.getActuator().isEnabled());
        assertTrue(druidStatProperties.getActuator().isBasic());
        assertTrue(druidStatProperties.getActuator().isDatasource());
        assertTrue(druidStatProperties.getActuator().isSql());
        assertTrue(druidStatProperties.getActuator().isWeburi());
        assertTrue(druidStatProperties.getActuator().isWebsession());
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
}