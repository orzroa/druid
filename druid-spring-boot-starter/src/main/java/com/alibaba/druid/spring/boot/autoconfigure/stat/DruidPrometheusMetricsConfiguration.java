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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;

/**
 * Registers an embedded Prometheus scrape endpoint for Druid statistics.
 *
 * @author druid
 */
@ConditionalOnWebApplication
@ConditionalOnProperty(name = "spring.datasource.druid.prometheus.enabled", havingValue = "true")
public class DruidPrometheusMetricsConfiguration {
    static final String DEFAULT_URL_PATTERN = "/actuator/prometheus";

    @Bean
    public ServletRegistrationBean druidPrometheusServletRegistrationBean(DruidStatProperties properties) {
        DruidStatProperties.Prometheus config = properties.getPrometheus();
        ServletRegistrationBean registrationBean = new ServletRegistrationBean();
        registrationBean.setName("druidPrometheusMetricsServlet");
        registrationBean.setServlet(new DruidPrometheusMetricsServlet(new DruidPrometheusMetricsExporter(config)));
        registrationBean.addUrlMappings(normalizeUrlPattern(config.getUrlPattern()));
        return registrationBean;
    }

    static String normalizeUrlPattern(String urlPattern) {
        if (urlPattern == null || urlPattern.trim().isEmpty()) {
            return DEFAULT_URL_PATTERN;
        }
        String normalized = urlPattern.trim();
        return normalized.charAt(0) == '/' ? normalized : "/" + normalized;
    }
}
