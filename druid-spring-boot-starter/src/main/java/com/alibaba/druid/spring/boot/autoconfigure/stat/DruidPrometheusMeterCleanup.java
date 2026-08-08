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

import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import io.micrometer.core.instrument.Meter;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 一期 SQL/URI 明细 Meter 的事件驱动清理器。
 *
 * <p>本类只负责清理调度、并发互斥和日志；具体 Meter 状态由 listener 通过
 * {@link Handler} 回调释放。正常事件路径只读取一个调度快照、调用
 * {@link Clock#millis()} 并做一次整数比较，不进行日期对象转换或临时对象分配。
 */
final class DruidPrometheusMeterCleanup {
    private static final java.util.logging.Logger LOG =
            java.util.logging.Logger.getLogger("druid.prometheus.cleanup");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final long BASELINE_UNINITIALIZED = Long.MIN_VALUE;
    private static final long CLEANUP_DISABLED = Long.MAX_VALUE;

    private final Clock clock;
    private final Handler handler;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicLong lastRemovalWarnMs = new AtomicLong(0L);

    private volatile long lastCleanupAtMillis = BASELINE_UNINITIALIZED;
    private volatile Schedule schedule = new Schedule(0, BASELINE_UNINITIALIZED);

    DruidPrometheusMeterCleanup(Clock clock, Handler handler) {
        this.clock = clock;
        this.handler = handler;
    }

    ReentrantReadWriteLock.ReadLock readLock() {
        return lock.readLock();
    }

    ReentrantReadWriteLock.WriteLock writeLock() {
        return lock.writeLock();
    }

    /** 初始化或独立更新周期。 */
    void updateInterval(int intervalHours) {
        lock.writeLock().lock();
        try {
            updateIntervalWhileLocked(intervalHours);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 调用方已持有 {@link #writeLock()} 时更新周期，用于和配置快照一起发布。 */
    void updateIntervalWhileLocked(int intervalHours) {
        int previous = schedule.intervalHours;
        if (intervalHours <= 0) {
            if (previous > 0) {
                LOG.warning("cleanup interval-hours=" + intervalHours + " <= 0, cleanup disabled");
            } else {
                LOG.info("cleanup interval-hours=" + intervalHours + " <= 0, cleanup disabled on startup");
            }
        }

        long nextCleanupAt;
        if (intervalHours <= 0) {
            nextCleanupAt = CLEANUP_DISABLED;
        } else {
            long last = lastCleanupAtMillis;
            nextCleanupAt = last == BASELINE_UNINITIALIZED
                    ? BASELINE_UNINITIALIZED
                    : nextCleanupBoundaryMillis(last, intervalHours);
        }
        schedule = new Schedule(intervalHours, nextCleanupAt);
    }

    void maybeCleanupSql(DataSourceProxy dataSource) {
        maybeCleanup("sql", dataSource);
    }

    void maybeCleanupUri(String uriTemplate) {
        maybeCleanup("uri", uriTemplate);
    }

    private void maybeCleanup(String triggerType, Object triggerSource) {
        Schedule current = schedule;
        int intervalHours = current.intervalHours;
        if (intervalHours <= 0) {
            return;
        }
        long nowMillis = clock.millis();
        long nextCleanupAt = current.nextCleanupAtMillis;
        if (nextCleanupAt != BASELINE_UNINITIALIZED && nowMillis < nextCleanupAt) {
            return;
        }

        CleanupResult result = null;
        lock.writeLock().lock();
        try {
            current = schedule;
            intervalHours = current.intervalHours;
            if (intervalHours <= 0) {
                return;
            }
            nowMillis = clock.millis();
            nextCleanupAt = current.nextCleanupAtMillis;
            if (nextCleanupAt == BASELINE_UNINITIALIZED) {
                lastCleanupAtMillis = nowMillis;
                schedule = new Schedule(intervalHours, nextCleanupBoundaryMillis(nowMillis, intervalHours));
                return;
            }
            if (nowMillis < nextCleanupAt) {
                return;
            }

            long startNs = System.nanoTime();
            CleanupCounts counts = handler.cleanup();
            long finishedMillis = clock.millis();
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
            String reason = cleanupReason(lastCleanupAtMillis, nowMillis);
            result = cleanupResult(nowMillis, finishedMillis, elapsedMs, reason, intervalHours, counts);
            lastCleanupAtMillis = nowMillis;
            schedule = new Schedule(intervalHours, nextCleanupBoundaryMillis(nowMillis, intervalHours));
        } finally {
            lock.writeLock().unlock();
        }

        try {
            logCleanup(result, triggerType, triggerSource);
        } catch (RuntimeException ignored) {
            // 清理日志及自定义日志 Handler 的异常不能影响指标事件主路径。
        }
    }

    void warnRemovalFailure(Meter.Id id, RuntimeException error) {
        long now = System.currentTimeMillis();
        long last = lastRemovalWarnMs.get();
        if (now - last >= 1000L && lastRemovalWarnMs.compareAndSet(last, now)) {
            LOG.warning("failed to remove meter " + id
                    + ", deferred to next cleanup cycle: " + error.getMessage());
        }
    }

    private long nextCleanupBoundaryMillis(long referenceMillis, int intervalHours) {
        ZoneId zone = clock.getZone();
        ZonedDateTime reference = Instant.ofEpochMilli(referenceMillis).atZone(zone);
        LocalDate date = reference.toLocalDate();
        if (intervalHours >= 24) {
            return date.plusDays(1).atStartOfDay(zone).toInstant().toEpochMilli();
        }
        int nextHour = ((reference.getHour() / intervalHours) + 1) * intervalHours;
        if (nextHour >= 24) {
            return date.plusDays(1).atStartOfDay(zone).toInstant().toEpochMilli();
        }
        return date.atTime(nextHour, 0).atZone(zone).toInstant().toEpochMilli();
    }

    private String cleanupReason(long lastMillis, long nowMillis) {
        ZoneId zone = clock.getZone();
        LocalDate lastDate = Instant.ofEpochMilli(lastMillis).atZone(zone).toLocalDate();
        LocalDate nowDate = Instant.ofEpochMilli(nowMillis).atZone(zone).toLocalDate();
        return lastDate.equals(nowDate) ? "new-interval" : "cross-day";
    }

    private CleanupResult cleanupResult(long startedMillis, long finishedMillis, long elapsedMs,
                                        String reason, int intervalHours, CleanupCounts counts) {
        ZonedDateTime started = Instant.ofEpochMilli(startedMillis).atZone(clock.getZone());
        int slot = intervalHours < 24 ? started.getHour() / intervalHours : 0;
        return new CleanupResult(startedMillis, finishedMillis, elapsedMs, reason, intervalHours,
                slot, slot * intervalHours, counts);
    }

    private void logCleanup(CleanupResult result, String triggerType, Object rawTriggerSource) {
        String triggerSource = "unknown";
        try {
            if ("sql".equals(triggerType) && rawTriggerSource instanceof DataSourceProxy) {
                triggerSource = handler.dataSourceName((DataSourceProxy) rawTriggerSource);
            } else if ("uri".equals(triggerType) && rawTriggerSource instanceof String) {
                triggerSource = (String) rawTriggerSource;
            }
        } catch (RuntimeException e) {
            LOG.warning("failed to resolve cleanup trigger source: " + e.getMessage());
        }

        ZoneId zone = clock.getZone();
        ZonedDateTime started = Instant.ofEpochMilli(result.startedMillis).atZone(zone);
        ZonedDateTime finished = Instant.ofEpochMilli(result.finishedMillis).atZone(zone);
        CleanupCounts counts = result.counts;
        LOG.info("druid detail meter cleanup: date=" + started.toLocalDate()
                + " zone=" + zone.getId()
                + " reason=" + result.reason
                + " boundary=" + String.format("%02d:00", result.boundaryHour)
                + " slot=" + result.slot
                + " interval-hours=" + result.intervalHours
                + " started=" + started.format(TIME_FORMATTER)
                + " finished=" + finished.format(TIME_FORMATTER)
                + " elapsedMs=" + result.elapsedMs
                + " sqlBefore=" + counts.sqlBefore
                + " uriBefore=" + counts.uriBefore
                + " sqlSuccess=" + counts.sqlSuccess
                + " sqlFailed=" + counts.sqlFailed
                + " uriSuccess=" + counts.uriSuccess
                + " uriFailed=" + counts.uriFailed
                + " retrySuccess=" + counts.retrySuccess
                + " retryFailed=" + counts.retryFailed
                + " triggerType=" + triggerType
                + " triggerSource=" + triggerSource);
    }

    interface Handler {
        CleanupCounts cleanup();

        String dataSourceName(DataSourceProxy dataSource);
    }

    static final class CleanupCounts {
        final int sqlBefore;
        final int uriBefore;
        final int sqlSuccess;
        final int sqlFailed;
        final int uriSuccess;
        final int uriFailed;
        final int retrySuccess;
        final int retryFailed;

        CleanupCounts(int sqlBefore, int uriBefore, int sqlSuccess, int sqlFailed,
                      int uriSuccess, int uriFailed, int retrySuccess, int retryFailed) {
            this.sqlBefore = sqlBefore;
            this.uriBefore = uriBefore;
            this.sqlSuccess = sqlSuccess;
            this.sqlFailed = sqlFailed;
            this.uriSuccess = uriSuccess;
            this.uriFailed = uriFailed;
            this.retrySuccess = retrySuccess;
            this.retryFailed = retryFailed;
        }
    }

    private static final class Schedule {
        private final int intervalHours;
        private final long nextCleanupAtMillis;

        private Schedule(int intervalHours, long nextCleanupAtMillis) {
            this.intervalHours = intervalHours;
            this.nextCleanupAtMillis = nextCleanupAtMillis;
        }
    }

    private static final class CleanupResult {
        private final long startedMillis;
        private final long finishedMillis;
        private final long elapsedMs;
        private final String reason;
        private final int intervalHours;
        private final int slot;
        private final int boundaryHour;
        private final CleanupCounts counts;

        private CleanupResult(long startedMillis, long finishedMillis, long elapsedMs, String reason,
                              int intervalHours, int slot, int boundaryHour, CleanupCounts counts) {
            this.startedMillis = startedMillis;
            this.finishedMillis = finishedMillis;
            this.elapsedMs = elapsedMs;
            this.reason = reason;
            this.intervalHours = intervalHours;
            this.slot = slot;
            this.boundaryHour = boundaryHour;
            this.counts = counts;
        }
    }
}
