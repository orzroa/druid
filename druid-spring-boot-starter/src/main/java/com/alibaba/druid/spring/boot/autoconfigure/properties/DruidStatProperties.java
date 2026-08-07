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
        private Thresholds thresholds = new Thresholds();
        private Logging logging = new Logging();

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

        public Thresholds getThresholds() {
            return thresholds;
        }

        public void setThresholds(Thresholds thresholds) {
            this.thresholds = thresholds;
        }

        public Logging getLogging() {
            return logging;
        }

        public void setLogging(Logging logging) {
            this.logging = logging;
        }

        public static class SqlMapping {
            /** Enable phase-one SQL mapping output when phase-two event logging is disabled. */
            private boolean enabled = true;
            /**
             * Legacy directory property retained for binding compatibility. SQL mappings now use
             * {@code prometheus.logging.directory}.
             */
            @Deprecated
            private String directory = "./logs/druid/sql-mapping";
            /** Capacity of the bounded SQL mapping writer queue; changes require a restart. */
            private int queueSize = 1000;

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            public String getDirectory() { return directory; }
            public void setDirectory(String directory) { this.directory = directory; }
            public int getQueueSize() { return queueSize; }
            public void setQueueSize(int queueSize) { this.queueSize = queueSize; }
        }

        public static class Events {
            /** Enable phase-one high-cardinality SQL and URI meters. */
            private boolean enabled = true;
            /** Maximum number of phase-one SQL identities retained by one application instance. */
            private int maxSqlIdentities = 1000;
            /** Maximum number of phase-one URI identities retained by one application instance. */
            private int maxUriIdentities = 1000;
            /** Distribution window used by newly created phase-one meters. */
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
            /** Include the servlet context path in phase-one URI template labels. */
            private boolean includeContextPath = false;

            public boolean isIncludeContextPath() { return includeContextPath; }
            public void setIncludeContextPath(boolean includeContextPath) {
                this.includeContextPath = includeContextPath;
            }
        }

        public static class Thresholds {
            /** Slow SQL threshold in milliseconds; zero disables the corresponding anomaly. */
            private long slowSqlMillis = 200L;
            /** Large SQL result-set row threshold; zero disables the corresponding anomaly. */
            private long largeSqlReadRows = 100L;
            /** Large SQL update row threshold; zero disables the corresponding anomaly. */
            private long largeSqlWriteRows = 10L;
            /** Slow URI threshold in milliseconds; zero disables the corresponding anomaly. */
            private long slowUriMillis = 1000L;
            /** Large per-request JDBC fetched-row threshold; zero disables the corresponding anomaly. */
            private long largeUriReadRows = 300L;
            /** Large per-request JDBC updated-row threshold; zero disables the corresponding anomaly. */
            private long largeUriWriteRows = 30L;
            /** Large per-request SQL execution threshold; zero disables the corresponding anomaly. */
            private long largeUriSqlExecutions = 20L;

            public long getSlowSqlMillis() { return slowSqlMillis; }
            public void setSlowSqlMillis(long slowSqlMillis) { this.slowSqlMillis = slowSqlMillis; }
            public long getLargeSqlReadRows() { return largeSqlReadRows; }
            public void setLargeSqlReadRows(long largeSqlReadRows) { this.largeSqlReadRows = largeSqlReadRows; }
            public long getLargeSqlWriteRows() { return largeSqlWriteRows; }
            public void setLargeSqlWriteRows(long largeSqlWriteRows) { this.largeSqlWriteRows = largeSqlWriteRows; }
            public long getSlowUriMillis() { return slowUriMillis; }
            public void setSlowUriMillis(long slowUriMillis) { this.slowUriMillis = slowUriMillis; }
            public long getLargeUriReadRows() { return largeUriReadRows; }
            public void setLargeUriReadRows(long largeUriReadRows) { this.largeUriReadRows = largeUriReadRows; }
            public long getLargeUriWriteRows() { return largeUriWriteRows; }
            public void setLargeUriWriteRows(long largeUriWriteRows) { this.largeUriWriteRows = largeUriWriteRows; }
            public long getLargeUriSqlExecutions() { return largeUriSqlExecutions; }
            public void setLargeUriSqlExecutions(long largeUriSqlExecutions) { this.largeUriSqlExecutions = largeUriSqlExecutions; }
        }

        public static class Logging {
            /** Enable phase-two structured SQL and URI event logging. */
            private boolean enabled = true;
            /** Automatically install the dedicated Logback appender when Logback is available. */
            private boolean autoConfigure = true;
            /** Sampling rate for normal events, clamped to the range {@code [0, 1]}. */
            private double normalSampleRate = 0.01D;
            /** Shared directory for event logs and {@code sql_mapping_<md5>.log} files. */
            private String directory = "./logs";
            /** Active structured event log file name. */
            private String fileName = "druid-metrics-events.log";
            /** Capacity of the dedicated asynchronous Logback queue. */
            private int queueSize = 4096;
            /** Maximum active log size before rollover. */
            private String maxFileSize = "10MB";
            /** Maximum number of days for which rolled event logs are retained. */
            private int maxHistoryDays = 7;
            /** Total size cap for the active and rolled event logs. */
            private String totalSizeCap = "1GB";
            /** Maximum time to flush the asynchronous queue during rebuild or shutdown. */
            private String shutdownFlushTimeout = "3s";

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            public boolean isAutoConfigure() { return autoConfigure; }
            public void setAutoConfigure(boolean autoConfigure) { this.autoConfigure = autoConfigure; }
            public double getNormalSampleRate() { return normalSampleRate; }
            public void setNormalSampleRate(double normalSampleRate) {
                if (Double.isNaN(normalSampleRate) || normalSampleRate < 0D) this.normalSampleRate = 0D;
                else if (normalSampleRate > 1D) this.normalSampleRate = 1D;
                else this.normalSampleRate = normalSampleRate;
            }
            public String getDirectory() { return directory; }
            public void setDirectory(String directory) { this.directory = directory; }
            public String getFileName() { return fileName; }
            public void setFileName(String fileName) { this.fileName = fileName; }
            public int getQueueSize() { return queueSize; }
            public void setQueueSize(int queueSize) { this.queueSize = queueSize; }
            public String getMaxFileSize() { return maxFileSize; }
            public void setMaxFileSize(String maxFileSize) { this.maxFileSize = maxFileSize; }
            public int getMaxHistoryDays() { return maxHistoryDays; }
            public void setMaxHistoryDays(int maxHistoryDays) { this.maxHistoryDays = maxHistoryDays; }
            public String getTotalSizeCap() { return totalSizeCap; }
            public void setTotalSizeCap(String totalSizeCap) { this.totalSizeCap = totalSizeCap; }
            public String getShutdownFlushTimeout() { return shutdownFlushTimeout; }
            public void setShutdownFlushTimeout(String shutdownFlushTimeout) { this.shutdownFlushTimeout = shutdownFlushTimeout; }
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
