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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
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
    public void testConfigurationUsesExistingRegistry() {
        DruidStatProperties properties = new DruidStatProperties();
        properties.getPrometheus().setBasic(false);
        properties.getPrometheus().setDatasource(false);
        properties.getPrometheus().setSql(false);
        properties.getPrometheus().setWeburi(false);
        properties.getPrometheus().setWebsession(false);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        DruidPrometheusMetricsExporter exporter = new DruidPrometheusMetricsConfiguration()
                .druidPrometheusMetricsExporter(properties, provider(registry));
        exporter.init();

        assertSame(registry, exporterMeterRegistry(exporter));
        exporter.destroy();
    }

    @Test
    public void testConfigurationWithoutRegistryDoesNotStart() {
        DruidStatProperties properties = new DruidStatProperties();
        DruidPrometheusMetricsExporter exporter = new DruidPrometheusMetricsConfiguration()
                .druidPrometheusMetricsExporter(properties, provider(null));

        exporter.init();

        assertNull(exporterMeterRegistry(exporter));
        exporter.destroy();
    }

    @Test
    public void testPrometheusPropertyDefaults() {
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        assertFalse(config.isEnabled());
        assertTrue(config.isBasic());
        assertTrue(config.isDatasource());
        assertTrue(config.isSql());
        assertTrue(config.isWeburi());
        assertTrue(config.isWebsession());
    }

    private static Object exporterMeterRegistry(DruidPrometheusMetricsExporter exporter) {
        try {
            java.lang.reflect.Field field =
                    DruidPrometheusMetricsExporter.class.getDeclaredField("meterRegistry");
            field.setAccessible(true);
            return field.get(exporter);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static <T> ObjectProvider<T> provider(final T value) {
        return new ObjectProvider<T>() {
            @Override
            public T getObject(Object... args) throws BeansException {
                return value;
            }

            @Override
            public T getIfAvailable() throws BeansException {
                return value;
            }

            @Override
            public T getIfUnique() throws BeansException {
                return value;
            }

            @Override
            public T getObject() throws BeansException {
                return value;
            }
        };
    }
}
