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
package com.alibaba.druid.spring.boot.autoconfigure.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author lihengming [89921218@qq.com]
 */
@ConfigurationProperties("spring.datasource.druid")
public class DruidStatProperties {
    private String[] aopPatterns;
    private StatViewServlet statViewServlet = new StatViewServlet();
    private WebStatFilter webStatFilter = new WebStatFilter();
    private Prometheus prometheus = new Prometheus();

    public String[] getAopPatterns() {
        return aopPatterns;
    }

    public void setAopPatterns(String[] aopPatterns) {
        this.aopPatterns = aopPatterns;
    }

    public StatViewServlet getStatViewServlet() {
        return statViewServlet;
    }

    public void setStatViewServlet(StatViewServlet statViewServlet) {
        this.statViewServlet = statViewServlet;
    }

    public WebStatFilter getWebStatFilter() {
        return webStatFilter;
    }

    public void setWebStatFilter(WebStatFilter webStatFilter) {
        this.webStatFilter = webStatFilter;
    }

    public Prometheus getPrometheus() {
        return prometheus;
    }

    public void setPrometheus(Prometheus prometheus) {
        this.prometheus = prometheus;
    }

    public static class StatViewServlet {
        /**
         * Enable StatViewServlet.
         */
        private boolean enabled = false;
        private String urlPattern;
        private String allow;
        private String deny;
        private String loginUsername;
        private String loginPassword;
        private String resetEnable;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getUrlPattern() {
            return urlPattern;
        }

        public void setUrlPattern(String urlPattern) {
            this.urlPattern = urlPattern;
        }

        public String getAllow() {
            return allow;
        }

        public void setAllow(String allow) {
            this.allow = allow;
        }

        public String getDeny() {
            return deny;
        }

        public void setDeny(String deny) {
            this.deny = deny;
        }

        public String getLoginUsername() {
            return loginUsername;
        }

        public void setLoginUsername(String loginUsername) {
            this.loginUsername = loginUsername;
        }

        public String getLoginPassword() {
            return loginPassword;
        }

        public void setLoginPassword(String loginPassword) {
            this.loginPassword = loginPassword;
        }

        public String getResetEnable() {
            return resetEnable;
        }

        public void setResetEnable(String resetEnable) {
            this.resetEnable = resetEnable;
        }
    }

    /**
     * Prometheus metrics export configuration.
     */
    public static class Prometheus {
        /**
         * Enable Prometheus metrics export.
         */
        private boolean enabled = true;
        private Events events = new Events();
        private SqlMapping sqlMapping = new SqlMapping();
        private UriTemplate uriTemplate = new UriTemplate();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Events getEvents() {
            return events;
        }

        public void setEvents(Events events) {
            this.events = events;
        }

        public SqlMapping getSqlMapping() {
            return sqlMapping;
        }

        public void setSqlMapping(SqlMapping sqlMapping) {
            this.sqlMapping = sqlMapping;
        }

        public UriTemplate getUriTemplate() {
            return uriTemplate;
        }

        public void setUriTemplate(UriTemplate uriTemplate) {
            this.uriTemplate = uriTemplate;
        }

        public static class SqlMapping {
            private boolean enabled = true;
            private String directory = "./logs/druid/sql-mapping";
            private int queueSize = 1000;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            public String getDirectory() { return directory; }
            public void setDirectory(String directory) { this.directory = directory; }
            public int getQueueSize() { return queueSize; }
            public void setQueueSize(int queueSize) { this.queueSize = queueSize; }
        }

        public static class Events {
            private boolean enabled = true;
            private int maxSqlIdentities = 1000;
            private int maxUriIdentities = 1000;
            private String maxWindow = "2m";

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            public int getMaxSqlIdentities() { return maxSqlIdentities; }
            public void setMaxSqlIdentities(int maxSqlIdentities) { this.maxSqlIdentities = maxSqlIdentities; }
            public int getMaxUriIdentities() { return maxUriIdentities; }
            public void setMaxUriIdentities(int maxUriIdentities) { this.maxUriIdentities = maxUriIdentities; }
            public String getMaxWindow() { return maxWindow; }
            public void setMaxWindow(String maxWindow) { this.maxWindow = maxWindow; }
        }

        public static class UriTemplate {
            private boolean includeContextPath = false;

            public boolean isIncludeContextPath() { return includeContextPath; }
            public void setIncludeContextPath(boolean includeContextPath) {
                this.includeContextPath = includeContextPath;
            }
        }

    }

    public static class WebStatFilter {
        /**
         * Enable WebStatFilter.
         */
        private boolean enabled = true;
        private String urlPattern;
        private String exclusions;
        private String sessionStatMaxCount;
        private String sessionStatEnable;
        private String principalSessionName;
        private String principalCookieName;
        private String profileEnable;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getUrlPattern() {
            return urlPattern;
        }

        public void setUrlPattern(String urlPattern) {
            this.urlPattern = urlPattern;
        }

        public String getExclusions() {
            return exclusions;
        }

        public void setExclusions(String exclusions) {
            this.exclusions = exclusions;
        }

        public String getSessionStatMaxCount() {
            return sessionStatMaxCount;
        }

        public void setSessionStatMaxCount(String sessionStatMaxCount) {
            this.sessionStatMaxCount = sessionStatMaxCount;
        }

        public String getSessionStatEnable() {
            return sessionStatEnable;
        }

        public void setSessionStatEnable(String sessionStatEnable) {
            this.sessionStatEnable = sessionStatEnable;
        }

        public String getPrincipalSessionName() {
            return principalSessionName;
        }

        public void setPrincipalSessionName(String principalSessionName) {
            this.principalSessionName = principalSessionName;
        }

        public String getPrincipalCookieName() {
            return principalCookieName;
        }

        public void setPrincipalCookieName(String principalCookieName) {
            this.principalCookieName = principalCookieName;
        }

        public String getProfileEnable() {
            return profileEnable;
        }

        public void setProfileEnable(String profileEnable) {
            this.profileEnable = profileEnable;
        }
    }
}
