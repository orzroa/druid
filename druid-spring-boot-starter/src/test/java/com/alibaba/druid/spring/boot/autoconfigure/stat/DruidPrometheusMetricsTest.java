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
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DruidPrometheusMetricsTest {
    private static final String HASH = "f15e5e09c27c92be6ed2b586d171d68a";

    private DruidStatProperties.Prometheus config;
    private StubStatService statService;

    @Before
    public void setUp() {
        config = new DruidStatProperties.Prometheus();
        config.setBasic(false);
        config.setDatasource(false);
        config.setWebsession(false);
        config.setSql(true);
        config.setWeburi(true);

        statService = new StubStatService();
        statService.add("/weburi.json", result("[{"
                + "\"URI\":\"/api/\\\"quoted\\\\path\\nnext\","
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

    @Test
    public void testAllDruid2PromMetricFamiliesAndValuesAreExported() {
        String metrics = new DruidPrometheusMetricsExporter(config, statService).scrape();

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
        for (String family : metricFamilies) {
            assertTrue("Missing metric family " + family,
                    metrics.contains("# TYPE " + family + " "));
        }
        assertEquals(19, count(metrics, "# TYPE druid_"));

        assertTrue(metrics.contains("druid_sql_execute_count_sum{sql=\"" + HASH + "\"} 4"));
        assertTrue(metrics.contains("druid_sql_execute_time_avg{sql=\"" + HASH + "\"} 2.5"));
        assertTrue(metrics.contains("druid_sql_execute_time_histogram{sql=\"" + HASH
                + "\",max=\"10000\"} 8"));
        assertTrue(metrics.contains("druid_sql_effect_row_histogram{sql=\"" + HASH
                + "\",max=\"99999\"} 4"));
        assertTrue(metrics.contains("druid_sql_fetch_row_histogram{sql=\"" + HASH
                + "\",max=\"99999\"} 9"));

        String escapedUri = "/api/\\\"quoted\\\\path\\nnext";
        assertTrue(metrics.contains("druid_uri_request_count_sum{uri=\"" + escapedUri + "\"} 4"));
        assertTrue(metrics.contains("druid_uri_request_time_avg{uri=\"" + escapedUri + "\"} 2.5"));
        assertFalse(metrics.contains("\nnext\""));
    }

    @Test
    public void testEachDruidEndpointIsReadOnlyOncePerScrape() {
        new DruidPrometheusMetricsExporter(config, statService).scrape();

        assertEquals(1, statService.calls("/sql.json"));
        assertEquals(1, statService.calls("/weburi.json"));
        assertEquals(0, statService.calls("/basic.json"));
        assertEquals(0, statService.calls("/datasource.json"));
        assertEquals(0, statService.calls("/websession.json"));
    }

    @Test
    public void testBasicAndDatasourceMetricsAreAggregatedFromDatasourceStats() {
        config.setSql(false);
        config.setWeburi(false);
        config.setBasic(true);
        config.setDatasource(true);
        statService.add("/datasource.json", result("["
                + "{\"ActiveCount\":2,\"PoolingCount\":3,\"MaxActive\":10,"
                + "\"ExecuteCount\":20,\"ErrorCount\":1,\"CommitCount\":4,"
                + "\"RollbackCount\":2,\"WaitThreadCount\":1,\"NotEmptyWaitCount\":5},"
                + "{\"ActiveCount\":1,\"PoolingCount\":4,\"MaxActive\":20,"
                + "\"ExecuteCount\":30,\"ErrorCount\":2,\"CommitCount\":6,"
                + "\"RollbackCount\":3,\"WaitThreadCount\":2,\"NotEmptyWaitCount\":7}"
                + "]"));

        String metrics = new DruidPrometheusMetricsExporter(config, statService).scrape();

        assertTrue(metrics.contains("druid_active_connections 3"));
        assertTrue(metrics.contains("druid_pooling_connections 7"));
        assertTrue(metrics.contains("druid_pooling_max_connections 30"));
        assertTrue(metrics.contains("druid_execute_count 50"));
        assertTrue(metrics.contains("druid_datasource_count 2"));
        assertTrue(metrics.contains("druid_datasource_active_connections 3"));
        assertEquals(1, statService.calls("/datasource.json"));
        assertEquals(0, statService.calls("/basic.json"));
    }

    @Test
    public void testZeroCountsAndShortHistogramsProduceValidNumbers() {
        statService.add("/weburi.json", result("[{"
                + "\"URI\":\"/empty\","
                + "\"RequestCount\":0,"
                + "\"RequestTimeMillis\":10,"
                + "\"Histogram\":[3]"
                + "}]"));
        statService.add("/sql.json", result("[{"
                + "\"SQL\":\"SELECT 1\","
                + "\"ExecuteCount\":0,"
                + "\"ExecuteAndResultSetHoldTime\":10,"
                + "\"ExecuteAndResultHoldTimeHistogram\":[]"
                + "}]"));

        String metrics = new DruidPrometheusMetricsExporter(config, statService).scrape();

        assertTrue(metrics.contains("druid_uri_request_time_avg{uri=\"/empty\"} 0"));
        assertTrue(metrics.contains("druid_uri_request_time_histogram{uri=\"/empty\",max=\"0.01\"} 0"));
        assertTrue(metrics.contains("druid_sql_execute_time_avg{sql=\"b1698e52a0f16203489454196a0c6307\"} 0"));
        assertFalse(metrics.contains("NaN"));
        assertFalse(metrics.contains("Infinity"));
    }

    @Test
    public void testMalformedOrFailedDruidResponsesDoNotBreakScrape() {
        statService.add("/weburi.json", "not-json");
        statService.add("/sql.json", "{\"ResultCode\":-1,\"Content\":[{\"SQL\":\"SELECT secret\"}]}");

        String metrics = new DruidPrometheusMetricsExporter(config, statService).scrape();

        assertEquals(19, count(metrics, "# TYPE druid_"));
        assertFalse(metrics.contains("secret"));
    }

    @Test
    public void testServletReturnsPrometheusContentTypeAndBody() throws Exception {
        DruidPrometheusMetricsServlet servlet =
                new DruidPrometheusMetricsServlet(new DruidPrometheusMetricsExporter(config, statService));
        MockHttpServletResponse response = new MockHttpServletResponse();

        servlet.doGet(new MockHttpServletRequest(), response);

        assertEquals(200, response.getStatus());
        assertEquals("text/plain; version=0.0.4; charset=utf-8", response.getContentType());
        assertTrue(response.getContentAsString().contains("druid_sql_execute_count_sum"));
    }

    private static String result(String content) {
        return "{\"ResultCode\":1,\"Content\":" + content + "}";
    }

    private static int count(String value, String token) {
        int result = 0;
        int offset = 0;
        while ((offset = value.indexOf(token, offset)) >= 0) {
            result++;
            offset += token.length();
        }
        return result;
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
