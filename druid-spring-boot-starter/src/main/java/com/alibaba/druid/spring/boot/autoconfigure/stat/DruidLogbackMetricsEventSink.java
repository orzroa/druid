/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 */
package com.alibaba.druid.spring.boot.autoconfigure.stat;

import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;
import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

/** Programmatic Logback appender owned only by the Druid metric event logger. */
final class DruidLogbackMetricsEventSink implements DruidMetricsEventSink {
    private static final Logger LOG = LoggerFactory.getLogger(DruidLogbackMetricsEventSink.class);
    private static final String LOGGER_NAME = "druid.metrics.event";
    private static final String APPENDER_NAME = "DRUID_METRICS_EVENT_ASYNC";

    private final LoggerContext context;
    private final ch.qos.logback.classic.Logger eventLogger;
    private final boolean originalAdditive;
    private final Level originalLevel;
    private final Object reconfigureLock = new Object();
    private volatile DruidStatProperties.Prometheus config;
    private volatile AppenderBundle bundle;
    private volatile ArrayBlockingQueue<Event> handoff;
    private final AtomicLong handoffDropped = new AtomicLong();

    DruidLogbackMetricsEventSink(DruidStatProperties.Prometheus config) {
        this.config = config;
        this.context = (LoggerContext) LoggerFactory.getILoggerFactory();
        this.eventLogger = context.getLogger(LOGGER_NAME);
        this.originalAdditive = eventLogger.isAdditive();
        this.originalLevel = eventLogger.getLevel();
    }

    @PostConstruct
    public void init() {
        if (config.getLogging().isAutoConfigure()) reconfigure(null, config);
    }

    @Override
    public void log(String json, boolean abnormal) {
        ArrayBlockingQueue<Event> queue = handoff;
        if (queue != null) {
            if (!queue.offer(new Event(json, abnormal))) handoffDropped.incrementAndGet();
            return;
        }
        if (config.getLogging().isAutoConfigure() && bundle == null) return;
        write(json, abnormal);
    }

    @Override
    public boolean isActive() {
        return !config.getLogging().isAutoConfigure() || bundle != null;
    }

    @Override
    public boolean refresh(DruidStatProperties.Prometheus previous, DruidStatProperties.Prometheus current) {
        this.config = current;
        if (!sameAppenderConfig(previous.getLogging(), current.getLogging())) return reconfigure(previous, current);
        return true;
    }

    private boolean reconfigure(DruidStatProperties.Prometheus previous,
                                DruidStatProperties.Prometheus current) {
        synchronized (reconfigureLock) {
            DruidStatProperties.Prometheus.Logging logging = current.getLogging();
            if (!logging.isAutoConfigure()) {
                stopOwnedAppender(logging.getShutdownFlushTimeout());
                eventLogger.setAdditive(originalAdditive);
                eventLogger.setLevel(originalLevel);
                return true;
            }
            try {
                validate(logging);
            } catch (RuntimeException e) {
                LOG.warn("Invalid Druid metric event Logback configuration; keeping previous appender", e);
                return false;
            }

            ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<Event>(Math.max(1, logging.getQueueSize()));
            handoff = queue;
            AppenderBundle previousBundle = bundle;
            stopOwnedAppender(previous == null ? logging.getShutdownFlushTimeout()
                    : previous.getLogging().getShutdownFlushTimeout());
            try {
                AppenderBundle next = startAppender(logging);
                eventLogger.setAdditive(false);
                eventLogger.setLevel(Level.INFO);
                eventLogger.addAppender(next.async);
                bundle = next;
                handoff = null;
                drain(queue);
                reportHandoffDrops();
                LOG.info("Druid metric event Logback appender configured: file={}", next.file);
                return true;
            } catch (RuntimeException e) {
                LOG.warn("Failed to start Druid metric event Logback appender; attempting rollback", e);
                bundle = null;
                if (previous != null && previous.getLogging().isAutoConfigure()) {
                    try {
                        AppenderBundle restored = startAppender(previous.getLogging());
                        eventLogger.setAdditive(false);
                        eventLogger.setLevel(Level.INFO);
                        eventLogger.addAppender(restored.async);
                        bundle = restored;
                    } catch (RuntimeException rollbackError) {
                        LOG.warn("Failed to restore previous Druid metric event appender; event logging disabled",
                                rollbackError);
                    }
                } else if (previousBundle != null) {
                    LOG.warn("Previous Druid metric event appender could not be restored");
                }
                handoff = null;
                drain(queue);
                reportHandoffDrops();
                return false;
            }
        }
    }

    private AppenderBundle startAppender(DruidStatProperties.Prometheus.Logging logging) {
        String directory = normalizedDirectory(logging.getDirectory());
        String fileName = normalizedFileName(logging.getFileName());
        String file = Paths.get(directory, fileName).toString();
        String stem = fileName.endsWith(".log") ? fileName.substring(0, fileName.length() - 4) : fileName;

        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%msg%n");
        encoder.start();

        RollingFileAppender<ILoggingEvent> rolling = new RollingFileAppender<ILoggingEvent>();
        rolling.setContext(context);
        rolling.setName("DRUID_METRICS_EVENT_FILE");
        rolling.setFile(file);
        rolling.setEncoder(encoder);

        SizeAndTimeBasedRollingPolicy<ILoggingEvent> policy = new SizeAndTimeBasedRollingPolicy<ILoggingEvent>();
        policy.setContext(context);
        policy.setParent(rolling);
        policy.setFileNamePattern(Paths.get(directory, stem + ".%d{yyyy-MM-dd}.%i.log").toString());
        policy.setMaxFileSize(FileSize.valueOf(logging.getMaxFileSize()));
        policy.setMaxHistory(Math.max(1, logging.getMaxHistoryDays()));
        policy.setTotalSizeCap(FileSize.valueOf(logging.getTotalSizeCap()));
        policy.start();
        rolling.setRollingPolicy(policy);
        rolling.start();
        if (!rolling.isStarted()) throw new IllegalStateException("RollingFileAppender did not start");

        AsyncAppender async = new AsyncAppender();
        async.setContext(context);
        async.setName(APPENDER_NAME);
        async.setQueueSize(Math.max(1, logging.getQueueSize()));
        async.setNeverBlock(true);
        async.setMaxFlushTime(toMillis(logging.getShutdownFlushTimeout(), 3000));
        async.addAppender(rolling);
        async.start();
        if (!async.isStarted()) {
            rolling.stop();
            throw new IllegalStateException("AsyncAppender did not start");
        }
        return new AppenderBundle(async, rolling, encoder, file);
    }

    private void stopOwnedAppender(String timeout) {
        AppenderBundle current = bundle;
        if (current == null) return;
        bundle = null;
        eventLogger.detachAppender(current.async);
        current.async.setMaxFlushTime(toMillis(timeout, 3000));
        current.async.stop();
        current.rolling.stop();
        current.encoder.stop();
    }

    private void drain(ArrayBlockingQueue<Event> queue) {
        Event event;
        while ((event = queue.poll()) != null) write(event.json, event.abnormal);
    }

    private void reportHandoffDrops() {
        long dropped = handoffDropped.getAndSet(0L);
        if (dropped > 0L) {
            LOG.warn("Druid metric event appender handoff queue was full; dropped {} complete events", dropped);
        }
    }

    private void write(String json, boolean abnormal) {
        if (abnormal) eventLogger.warn(json); else eventLogger.info(json);
    }

    private static void validate(DruidStatProperties.Prometheus.Logging logging) {
        normalizedFileName(logging.getFileName());
        String directory = normalizedDirectory(logging.getDirectory());
        try {
            Files.createDirectories(Paths.get(directory));
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot create event logging directory " + directory, e);
        }
        if (logging.getQueueSize() <= 0) throw new IllegalArgumentException("queue-size must be positive");
        if (logging.getMaxHistoryDays() <= 0) throw new IllegalArgumentException("max-history-days must be positive");
        FileSize.valueOf(logging.getMaxFileSize());
        FileSize.valueOf(logging.getTotalSizeCap());
        toMillis(logging.getShutdownFlushTimeout(), -1);
    }

    private static String normalizedDirectory(String value) {
        return value == null || value.trim().length() == 0 ? "./logs" : value.trim();
    }

    private static String normalizedFileName(String value) {
        String name = value == null || value.trim().length() == 0 ? "druid-metrics-events.log" : value.trim();
        Path path = Paths.get(name);
        if (path.getNameCount() != 1 || !path.getFileName().toString().equals(name)) {
            throw new IllegalArgumentException("logging.file-name must be a plain file name");
        }
        return name;
    }

    private static int toMillis(String value, int fallback) {
        if (value == null) return fallback;
        String text = value.trim().toLowerCase();
        try {
            long millis;
            if (text.endsWith("ms")) millis = Long.parseLong(text.substring(0, text.length() - 2));
            else if (text.endsWith("s")) millis = TimeUnit.SECONDS.toMillis(Long.parseLong(text.substring(0, text.length() - 1)));
            else if (text.endsWith("m")) millis = TimeUnit.MINUTES.toMillis(Long.parseLong(text.substring(0, text.length() - 1)));
            else millis = Long.parseLong(text);
            if (millis < 0L || millis > Integer.MAX_VALUE) throw new IllegalArgumentException("invalid duration " + value);
            return (int) millis;
        } catch (NumberFormatException e) {
            if (fallback >= 0) return fallback;
            throw new IllegalArgumentException("invalid duration " + value, e);
        }
    }

    private static boolean sameAppenderConfig(DruidStatProperties.Prometheus.Logging first,
                                              DruidStatProperties.Prometheus.Logging second) {
        return first.isAutoConfigure() == second.isAutoConfigure()
                && equal(first.getDirectory(), second.getDirectory())
                && equal(first.getFileName(), second.getFileName())
                && first.getQueueSize() == second.getQueueSize()
                && equal(first.getMaxFileSize(), second.getMaxFileSize())
                && first.getMaxHistoryDays() == second.getMaxHistoryDays()
                && equal(first.getTotalSizeCap(), second.getTotalSizeCap())
                && equal(first.getShutdownFlushTimeout(), second.getShutdownFlushTimeout());
    }

    private static boolean equal(Object first, Object second) {
        return first == null ? second == null : first.equals(second);
    }

    @Override
    public void close() {
        synchronized (reconfigureLock) {
            stopOwnedAppender(config.getLogging().getShutdownFlushTimeout());
            eventLogger.setAdditive(originalAdditive);
            eventLogger.setLevel(originalLevel);
        }
    }

    private static final class Event {
        private final String json;
        private final boolean abnormal;
        private Event(String json, boolean abnormal) { this.json = json; this.abnormal = abnormal; }
    }

    private static final class AppenderBundle {
        private final AsyncAppender async;
        private final RollingFileAppender<ILoggingEvent> rolling;
        private final PatternLayoutEncoder encoder;
        private final String file;
        private AppenderBundle(AsyncAppender async, RollingFileAppender<ILoggingEvent> rolling,
                               PatternLayoutEncoder encoder, String file) {
            this.async = async;
            this.rolling = rolling;
            this.encoder = encoder;
            this.file = file;
        }
    }
}
