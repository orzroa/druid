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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /** 明细指标排障日志；仅在 TRACE 级别下输出调度判定过程。 */
    private static final Logger DETAIL_LOG = LoggerFactory.getLogger("druid.prometheus.detail");
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

    /** 创建使用指定时钟和实际清理回调的事件驱动调度器。 */
    DruidPrometheusMeterCleanup(Clock clock, Handler handler) {
        this.clock = clock;
        this.handler = handler;
    }

    /** 返回事件记录路径使用的读锁，防止记录过程与批量清理并发。 */
    ReentrantReadWriteLock.ReadLock readLock() {
        return lock.readLock();
    }

    /** 返回配置刷新和批量清理使用的写锁。 */
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
        // 输出配置变更后计算出的下一次清理时间，便于排查动态刷新。
        if (DETAIL_LOG.isTraceEnabled()) {
            DETAIL_LOG.trace("druid detail meter cleanup schedule: action=update intervalHours={} nextCleanupAtMillis={} lastCleanupAtMillis={}",
                    intervalHours, nextCleanupAt, lastCleanupAtMillis);
        }
    }

    /** 在 SQL 事件进入记录路径前检查并按需触发清理。 */
    void maybeCleanupSql(DataSourceProxy dataSource) {
        maybeCleanup("sql", dataSource);
    }

    /** 在 URI 事件进入记录路径前检查并按需触发清理。 */
    void maybeCleanupUri(String uriTemplate) {
        maybeCleanup("uri", uriTemplate);
    }

    /**
     * 使用无锁快照完成快速判定，到期后在写锁内二次确认并执行一次批量清理。
     * 首次事件只建立时间基线，实际清理由后续跨边界事件触发。
     */
    private void maybeCleanup(String triggerType, Object triggerSource) {
        Schedule current = schedule;
        int intervalHours = current.intervalHours;
        if (intervalHours <= 0) {
            // 记录因清理开关关闭而跳过本次事件的原因。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail meter cleanup check: triggerType={} result=disabled intervalHours={}",
                        triggerType, intervalHours);
            }
            return;
        }
        long nowMillis = clock.millis();
        long nextCleanupAt = current.nextCleanupAtMillis;
        if (nextCleanupAt != BASELINE_UNINITIALIZED && nowMillis < nextCleanupAt) {
            // 记录当前事件尚未到达清理边界，无需申请写锁。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail meter cleanup check: triggerType={} result=before-boundary nowMillis={} nextCleanupAtMillis={}",
                        triggerType, nowMillis, nextCleanupAt);
            }
            return;
        }

        CleanupResult result = null;
        lock.writeLock().lock();
        try {
            current = schedule;
            intervalHours = current.intervalHours;
            if (intervalHours <= 0) {
                // 写锁等待期间配置可能被刷新，再次确认清理仍处于关闭状态。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail meter cleanup check: triggerType={} result=disabled-after-lock intervalHours={}",
                            triggerType, intervalHours);
                }
                return;
            }
            nowMillis = clock.millis();
            nextCleanupAt = current.nextCleanupAtMillis;
            if (nextCleanupAt == BASELINE_UNINITIALIZED) {
                lastCleanupAtMillis = nowMillis;
                schedule = new Schedule(intervalHours, nextCleanupBoundaryMillis(nowMillis, intervalHours));
                // 首个事件只建立时间基准，不执行历史 Meter 清理。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail meter cleanup check: triggerType={} result=baseline-initialized nowMillis={} nextCleanupAtMillis={}",
                            triggerType, nowMillis, schedule.nextCleanupAtMillis);
                }
                return;
            }
            if (nowMillis < nextCleanupAt) {
                // 其他线程已在等待写锁期间完成清理，当前事件无需重复执行。
                if (DETAIL_LOG.isTraceEnabled()) {
                    DETAIL_LOG.trace("druid detail meter cleanup check: triggerType={} result=already-handled nowMillis={} nextCleanupAtMillis={}",
                            triggerType, nowMillis, nextCleanupAt);
                }
                return;
            }

            // 记录触发清理的边界和事件类型，便于定位调度是否符合预期。
            if (DETAIL_LOG.isTraceEnabled()) {
                DETAIL_LOG.trace("druid detail meter cleanup check: triggerType={} result=triggered nowMillis={} boundaryMillis={} intervalHours={}",
                        triggerType, nowMillis, nextCleanupAt, intervalHours);
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

    /**
     * 在写锁内立即执行一次实际清理并输出 manual 日志。
     * 本方法有意不读写 lastCleanupAtMillis 和 schedule，保证人工操作不改变自动调度。
     */
    CleanupCounts cleanupNow() {
        CleanupResult result;
        lock.writeLock().lock();
        try {
            long startedMillis = clock.millis();
            long startNs = System.nanoTime();
            CleanupCounts counts = handler.cleanup();
            long finishedMillis = clock.millis();
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
            result = new CleanupResult(startedMillis, finishedMillis, elapsedMs, "manual",
                    schedule.intervalHours, -1, -1, counts);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            logCleanup(result, "manual", "cleanupNow");
        } catch (RuntimeException ignored) {
            // 手动清理已经完成，日志及自定义日志 Handler 的异常不能改变返回结果。
        }
        return result.counts;
    }

    /** 对 LRU 单 Meter 清理失败进行一秒限频告警，不保留逐项重试状态。 */
    void warnRemovalFailure(Meter.Id id, RuntimeException error) {
        long now = System.currentTimeMillis();
        long last = lastRemovalWarnMs.get();
        if (now - last >= 1000L && lastRemovalWarnMs.compareAndSet(last, now)) {
            LOG.warning("failed to remove meter " + id
                    + "; no per-meter retry is retained, the next periodic cleanup clears families and rescans registries: "
                    + error.getMessage());
        }
    }

    /** 根据应用时区计算引用时间之后的下一个整点清理边界。 */
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

    /** 判断本次清理是同日新区间还是跨日触发。 */
    private String cleanupReason(long lastMillis, long nowMillis) {
        ZoneId zone = clock.getZone();
        LocalDate lastDate = Instant.ofEpochMilli(lastMillis).atZone(zone).toLocalDate();
        LocalDate nowDate = Instant.ofEpochMilli(nowMillis).atZone(zone).toLocalDate();
        return lastDate.equals(nowDate) ? "new-interval" : "cross-day";
    }

    /** 汇总清理时间、区间槽位及删除计数，供锁外日志输出。 */
    private CleanupResult cleanupResult(long startedMillis, long finishedMillis, long elapsedMs,
                                        String reason, int intervalHours, CleanupCounts counts) {
        ZonedDateTime started = Instant.ofEpochMilli(startedMillis).atZone(clock.getZone());
        int slot = intervalHours < 24 ? started.getHour() / intervalHours : 0;
        return new CleanupResult(startedMillis, finishedMillis, elapsedMs, reason, intervalHours,
                slot, slot * intervalHours, counts);
    }

    /** 输出一次批量清理的完整结果；触发来源解析失败不会改变清理结果。 */
    private void logCleanup(CleanupResult result, String triggerType, Object rawTriggerSource) {
        String triggerSource = "unknown";
        try {
            if ("sql".equals(triggerType) && rawTriggerSource instanceof DataSourceProxy) {
                triggerSource = handler.dataSourceName((DataSourceProxy) rawTriggerSource);
            } else if ("uri".equals(triggerType) && rawTriggerSource instanceof String) {
                triggerSource = (String) rawTriggerSource;
            } else if ("manual".equals(triggerType) && rawTriggerSource instanceof String) {
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
                + " boundary=" + (result.boundaryHour < 0
                        ? "manual" : String.format("%02d:00", result.boundaryHour))
                + " slot=" + (result.slot < 0 ? "manual" : result.slot)
                + " interval-hours=" + result.intervalHours
                + " started=" + started.format(TIME_FORMATTER)
                + " finished=" + finished.format(TIME_FORMATTER)
                + " elapsedMs=" + result.elapsedMs
                + " familySuccess=" + counts.familySuccess
                + " sqlIdentityBefore=" + counts.sqlIdentityBefore
                + " uriIdentityBefore=" + counts.uriIdentityBefore
                + " sqlMeterBefore=" + counts.sqlMeterBefore
                + " uriMeterBefore=" + counts.uriMeterBefore
                + " sqlMeterSuccess=" + counts.sqlMeterSuccess
                + " sqlMeterFailed=" + counts.sqlMeterFailed
                + " uriMeterSuccess=" + counts.uriMeterSuccess
                + " uriMeterFailed=" + counts.uriMeterFailed
                + " triggerType=" + triggerType
                + " triggerSource=" + triggerSource);
    }

    interface Handler {
        /** 在调度器写锁内执行实际的 family、Meter 和本地状态清理。 */
        CleanupCounts cleanup();

        /** 将 SQL 触发源转换为便于排障的数据源名称。 */
        String dataSourceName(DataSourceProxy dataSource);
    }

    static final class CleanupCounts {
        final boolean familySuccess;
        final int sqlIdentityBefore;
        final int uriIdentityBefore;
        final int sqlMeterBefore;
        final int uriMeterBefore;
        final int sqlMeterSuccess;
        final int sqlMeterFailed;
        final int uriMeterSuccess;
        final int uriMeterFailed;

        /** 保存一次清理前的规模及执行结果，避免日志阶段重新扫描 Registry。 */
        CleanupCounts(boolean familySuccess, int sqlIdentityBefore, int uriIdentityBefore,
                      int sqlMeterBefore, int uriMeterBefore, int sqlMeterSuccess,
                      int sqlMeterFailed, int uriMeterSuccess, int uriMeterFailed) {
            this.familySuccess = familySuccess;
            this.sqlIdentityBefore = sqlIdentityBefore;
            this.uriIdentityBefore = uriIdentityBefore;
            this.sqlMeterBefore = sqlMeterBefore;
            this.uriMeterBefore = uriMeterBefore;
            this.sqlMeterSuccess = sqlMeterSuccess;
            this.sqlMeterFailed = sqlMeterFailed;
            this.uriMeterSuccess = uriMeterSuccess;
            this.uriMeterFailed = uriMeterFailed;
        }
    }

    private static final class Schedule {
        private final int intervalHours;
        private final long nextCleanupAtMillis;

        /** 创建不可变调度快照，供事件热路径无锁读取。 */
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

        /** 创建一次已完成清理的不可变结果快照。 */
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
