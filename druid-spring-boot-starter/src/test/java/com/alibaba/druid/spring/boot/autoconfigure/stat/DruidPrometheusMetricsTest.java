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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DruidPrometheusMetricsTest {
    private static final String HASH = "f15e5e09c27c92be6ed2b586d171d68a";

    private DruidStatProperties.Prometheus config;
    private SimpleMeterRegistry registry;
    private StubStatService statService;

    @Before
    public void setUp() {
        config = new DruidStatProperties.Prometheus();
        config.setSql(true);
        config.setWeburi(true);
        registry = new SimpleMeterRegistry();
        statService = new StubStatService();
        addDruid2PromFixtures();
    }

    @Test
    public void testAllDruid2PromMetricFamiliesAndValuesAreRegistered() {
        exporter().refresh();

        String[] metricFamilies = {
                "druid_uri_request_count_sum",
                "druid_uri_request_time_sum",
                "druid_uri_request_time_max",
                "druid_uri_request_time_avg",
                "druid_uri_request_time_histogram",
                "druid_uri_jdbc_execute_time_peak",
                "druid_uri_jdbc_fetch_row_peak",
                "druid_uri_jdbc_effect_row_peak",
                "druid_sql_execute_count_sum",
                "druid_sql_execute_time_sum",
                "druid_sql_execute_time_max",
                "druid_sql_execute_time_avg",
                "druid_sql_execute_time_histogram",
                "druid_sql_effect_row_sum",
                "druid_sql_effect_row_max",
                "druid_sql_effect_row_histogram",
                "druid_sql_fetch_row_sum",
                "druid_sql_fetch_row_max",
                "druid_sql_fetch_row_histogram"
        };
        Set<String> names = new HashSet<String>();
        for (Meter meter : registry.getMeters()) {
            names.add(meter.getId().getName());
        }
        for (String family : metricFamilies) {
            assertNotNull("Missing metric family " + family, registry.find(family).meter());
        }
        assertEquals(19, names.size());

        assertGauge(4, "druid_sql_execute_count_sum", "sql", HASH);
        assertGauge(2.5, "druid_sql_execute_time_avg", "sql", HASH);
        assertGauge(8, "druid_sql_execute_time_histogram",
                "sql", HASH, "max", "10000");
        assertGauge(4, "druid_sql_effect_row_histogram",
                "sql", HASH, "max", "99999");
        assertGauge(9, "druid_sql_fetch_row_histogram",
                "sql", HASH, "max", "99999");
        assertGauge(4, "druid_uri_request_count_sum", "uri", "/api/test");
        assertGauge(2.5, "druid_uri_request_time_avg", "uri", "/api/test");
    }

    @Test
    public void testEachDruidEndpointIsReadOnlyOncePerRefresh() {
        exporter().refresh();

        assertEquals(1, statService.calls("/sql.json"));
        assertEquals(1, statService.calls("/weburi.json"));
        assertEquals(0, statService.calls("/datasource.json"));
        assertEquals(0, statService.calls("/websession.json"));
    }

    @Test
    public void testZeroCountsAndShortHistogramsProduceValidValues() {
        statService.add("/weburi.json", result("[{"
                + "\"URI\":\"/empty\",\"RequestCount\":0,"
                + "\"RequestTimeMillis\":10,\"Histogram\":[3]}]"));
        statService.add("/sql.json", result("[{"
                + "\"SQL\":\"SELECT 1\",\"ExecuteCount\":0,"
                + "\"ExecuteAndResultSetHoldTime\":10,"
                + "\"ExecuteAndResultHoldTimeHistogram\":[]}]"));

        exporter().refresh();

        assertGauge(0, "druid_uri_request_time_avg", "uri", "/empty");
        assertGauge(0, "druid_uri_request_time_histogram",
                "uri", "/empty", "max", "0.01");
        assertGauge(0, "druid_sql_execute_time_avg",
                "sql", "b1698e52a0f16203489454196a0c6307");
    }

    @Test
    public void testMalformedResponseKeepsLastGoodValues() {
        DruidPrometheusMetricsExporter exporter = exporter();
        exporter.refresh();
        statService.add("/sql.json", "not-json");

        exporter.refresh();

        assertGauge(4, "druid_sql_execute_count_sum", "sql", HASH);
    }

    @Test
    public void testDisappearedDynamicSeriesAreRemoved() {
        DruidPrometheusMetricsExporter exporter = exporter();
        exporter.refresh();
        statService.add("/sql.json", result("[]"));

        exporter.refresh();

        assertNull(registry.find("druid_sql_execute_count_sum").tag("sql", HASH).meter());
        assertNotNull(registry.find("druid_uri_request_count_sum").tag("uri", "/api/test").meter());
    }

    @Test
    public void testDestroyRemovesOnlyOwnedMeters() {
        Gauge external = Gauge.builder("external_metric", new AtomicNumber(7),
                value -> value.value).register(registry);
        DruidPrometheusMetricsExporter exporter = exporter();
        exporter.refresh();

        exporter.destroy();

        assertNotNull(registry.find("external_metric").meter());
        assertEquals(7, external.value(), 0);
        assertNull(registry.find("druid_sql_execute_count_sum").meter());
    }

    @Test
    public void testExistingBusinessMetricIsNotOverwrittenOrRemoved() {
        AtomicNumber existingValue = new AtomicNumber(99);
        Gauge existing = Gauge.builder("druid_sql_execute_count_sum", existingValue,
                value -> value.value).tag("sql", HASH).register(registry);
        DruidPrometheusMetricsExporter exporter = exporter();

        exporter.refresh();
        exporter.destroy();

        assertEquals(99, existing.value(), 0);
        assertNotNull(registry.find("druid_sql_execute_count_sum").tag("sql", HASH).meter());
    }

    @Test
    public void testPrometheusRegistryScrapeContainsDruidMetrics() {
        PrometheusMeterRegistry prometheusRegistry =
                new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        DruidPrometheusMetricsExporter exporter =
                new DruidPrometheusMetricsExporter(config, prometheusRegistry, statService);

        exporter.refresh();
        String scrape = prometheusRegistry.scrape();

        assertNotNull(prometheusRegistry.find("druid_sql_execute_count_sum")
                .tag("sql", HASH).gauge());
        org.junit.Assert.assertTrue(scrape.contains(
                "druid_sql_execute_count_sum{sql=\"" + HASH + "\",} 4.0"));
        org.junit.Assert.assertTrue(scrape.contains(
                "druid_uri_request_count_sum{uri=\"/api/test\",} 4.0"));
    }

    private DruidPrometheusMetricsExporter exporter() {
        return new DruidPrometheusMetricsExporter(config, registry, statService);
    }

    private void assertGauge(double expected, String name, String... tags) {
        Gauge gauge = registry.find(name).tags(tags).gauge();
        assertNotNull("Missing gauge " + name, gauge);
        assertFalse("Gauge must not be NaN: " + name, Double.isNaN(gauge.value()));
        assertEquals(expected, gauge.value(), 0.000001);
    }

    private void addDruid2PromFixtures() {
        statService.add("/weburi.json", result("[{"
                + "\"URI\":\"/api/test\","
                + "\"RequestCount\":4,"
                + "\"RequestTimeMillis\":10,"
                + "\"RequestTimeMillisMax\":7,"
                + "\"Histogram\":[1,2,3,4,5,6,7,8],"
                + "\"JdbcExecutePeak\":9,"
                + "\"JdbcFetchRowPeak\":10,"
                + "\"JdbcUpdatePeak\":11"
                + "}]"));
        statService.add("/sql.json", result("[{"
                + "\"SQL\":\"SELECT * FROM users WHERE id = ?\","
                + "\"ExecuteCount\":4,"
                + "\"ExecuteAndResultSetHoldTime\":10,"
                + "\"MaxTimespan\":6,"
                + "\"ExecuteAndResultHoldTimeHistogram\":[1,2,3,4,5,6,7,8],"
                + "\"EffectedRowCount\":12,"
                + "\"EffectedRowCountMax\":5,"
                + "\"EffectedRowCountHistogram\":[9,8,7,6,5,4],"
                + "\"FetchRowCount\":20,"
                + "\"FetchRowCountMax\":8,"
                + "\"FetchRowCountHistogram\":[4,5,6,7,8,9]"
                + "}]"));
    }

    private static String result(String content) {
        return "{\"ResultCode\":1,\"Content\":" + content + "}";
    }

    private static final class AtomicNumber {
        private volatile double value;

        private AtomicNumber(double value) {
            this.value = value;
        }
    }

    private static final class StubStatService implements DruidPrometheusMetricsExporter.StatService {
        private final Map<String, String> responses = new HashMap<String, String>();
        private final Map<String, Integer> callCounts = new HashMap<String, Integer>();

        void add(String path, String response) {
            responses.put(path, response);
        }

        int calls(String path) {
            Integer count = callCounts.get(path);
            return count == null ? 0 : count.intValue();
        }

        @Override
        public String service(String path) {
            callCounts.put(path, Integer.valueOf(calls(path) + 1));
            return responses.get(path);
        }
    }
}
