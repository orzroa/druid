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
import org.junit.Test;
import org.springframework.boot.web.servlet.ServletRegistrationBean;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DruidPrometheusMetricsSimpleTest {
    @Test
    public void testSqlMd5MatchesDruid2Prom() {
        assertEquals("f15e5e09c27c92be6ed2b586d171d68a",
                DruidPrometheusMetricsExporter.calculateSqlMd5("SELECT * FROM users WHERE id = ?"));
        assertEquals("", DruidPrometheusMetricsExporter.calculateSqlMd5(null));
        assertEquals("", DruidPrometheusMetricsExporter.calculateSqlMd5("  "));
    }

    @Test
    public void testBucketLabelsMatchDruid2Prom() {
        String[] timeBuckets = {"0.001", "0.01", "0.1", "1", "10", "100", "1000", "10000"};
        for (int i = 0; i < timeBuckets.length; i++) {
            assertEquals(timeBuckets[i], DruidPrometheusMetricsExporter.getTimeBucketLabel(i));
        }

        String[] countBuckets = {"0", "9", "99", "999", "9999", "99999"};
        for (int i = 0; i < countBuckets.length; i++) {
            assertEquals(countBuckets[i], DruidPrometheusMetricsExporter.getCountBucketLabel(i));
        }
    }

    @Test
    public void testUrlPatternNormalization() {
        assertEquals("/actuator/prometheus",
                DruidPrometheusMetricsConfiguration.normalizeUrlPattern(null));
        assertEquals("/actuator/prometheus",
                DruidPrometheusMetricsConfiguration.normalizeUrlPattern("  "));
        assertEquals("/metrics",
                DruidPrometheusMetricsConfiguration.normalizeUrlPattern("metrics"));
        assertEquals("/custom/metrics",
                DruidPrometheusMetricsConfiguration.normalizeUrlPattern(" /custom/metrics "));
    }

    @Test
    public void testServletRegistrationUsesConfiguredPath() {
        DruidStatProperties properties = new DruidStatProperties();
        properties.getPrometheus().setUrlPattern("/custom/prometheus");

        ServletRegistrationBean bean = new DruidPrometheusMetricsConfiguration()
                .druidPrometheusServletRegistrationBean(properties);
        Collection<String> mappings = bean.getUrlMappings();

        assertEquals("druidPrometheusMetricsServlet", bean.getServletName());
        assertTrue(mappings.contains("/custom/prometheus"));
    }

    @Test
    public void testPrometheusPropertyDefaults() {
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        assertFalse(config.isEnabled());
        assertEquals("/actuator/prometheus", config.getUrlPattern());
        assertTrue(config.isBasic());
        assertTrue(config.isDatasource());
        assertTrue(config.isSql());
        assertTrue(config.isWeburi());
        assertTrue(config.isWebsession());
    }
}
