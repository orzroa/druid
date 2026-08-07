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
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * Registers Druid metrics in the application's existing Micrometer registry.
 *
 * @author druid
 */
@ConditionalOnClass(MeterRegistry.class)
public class DruidPrometheusMetricsConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(DruidPrometheusMetricsConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(DruidMetricsEventSink.class)
    public DruidMetricsEventSink druidMetricsEventSink(DruidStatProperties properties) {
        if ("ch.qos.logback.classic.LoggerContext".equals(LoggerFactory.getILoggerFactory().getClass().getName())) {
            try {
                Class<?> sinkClass = Class.forName(
                        "com.alibaba.druid.spring.boot.autoconfigure.stat.DruidLogbackMetricsEventSink");
                Constructor<?> constructor = sinkClass.getDeclaredConstructor(DruidStatProperties.Prometheus.class);
                return (DruidMetricsEventSink) constructor.newInstance(properties.getPrometheus());
            } catch (ReflectiveOperationException e) {
                LOG.warn("Failed to create Druid Logback metric event sink; using SLF4J fallback", e);
            }
        }
        return new DruidSlf4jMetricsEventSink(properties.getPrometheus());
    }

    @Bean
    @ConditionalOnMissingBean
    public DruidPrometheusMetricsListener druidPrometheusMetricsListener(
            DruidStatProperties properties, ObjectProvider<MeterRegistry> meterRegistryProvider,
            ObjectProvider<DruidUriTemplateResolver> uriTemplateResolverProvider,
            ObjectProvider<DruidMetricsEventSink> eventSinkProvider) {
        return new DruidPrometheusMetricsListener(properties.getPrometheus(), meterRegistryProvider,
                uriTemplateResolverProvider, eventSinkProvider.getIfAvailable());
    }
}
