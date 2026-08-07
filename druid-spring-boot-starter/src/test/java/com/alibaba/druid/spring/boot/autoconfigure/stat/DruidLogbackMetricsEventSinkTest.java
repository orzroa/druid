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

import com.alibaba.druid.spring.boot.autoconfigure.properties.DruidStatProperties;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DruidLogbackMetricsEventSinkTest {
    @Test
    public void autoConfigurationWritesCompleteJsonToConfiguredFile() throws Exception {
        Path directory = Files.createTempDirectory("druid-logback-events-");
        DruidLogbackMetricsEventSink sink = null;
        try {
            DruidStatProperties.Prometheus config = config(directory, "events.log");
            sink = new DruidLogbackMetricsEventSink(config);
            sink.init();

            assertTrue(sink.isActive());
            sink.log("{\"event\":\"normal\"}", false);
            sink.log("{\"event\":\"abnormal\"}", true);
            sink.close();
            sink = null;

            String text = read(directory.resolve("events.log"));
            assertTrue(text.contains("{\"event\":\"normal\"}"));
            assertTrue(text.contains("{\"event\":\"abnormal\"}"));
        } finally {
            if (sink != null) sink.close();
            delete(directory);
        }
    }

    @Test
    public void refreshSeriallySwitchesToNewDirectory() throws Exception {
        Path oldDirectory = Files.createTempDirectory("druid-logback-old-");
        Path newDirectory = Files.createTempDirectory("druid-logback-new-");
        DruidLogbackMetricsEventSink sink = null;
        try {
            DruidStatProperties.Prometheus oldConfig = config(oldDirectory, "events.log");
            sink = new DruidLogbackMetricsEventSink(oldConfig);
            sink.init();
            sink.log("{\"sequence\":1}", false);

            DruidStatProperties.Prometheus newConfig = config(newDirectory, "events.log");
            sink.refresh(oldConfig, newConfig);
            sink.log("{\"sequence\":2}", false);
            sink.close();
            sink = null;

            String oldText = read(oldDirectory.resolve("events.log"));
            String newText = read(newDirectory.resolve("events.log"));
            assertTrue(oldText.contains("{\"sequence\":1}"));
            assertFalse(oldText.contains("{\"sequence\":2}"));
            assertTrue(newText.contains("{\"sequence\":2}"));
        } finally {
            if (sink != null) sink.close();
            delete(oldDirectory);
            delete(newDirectory);
        }
    }

    @Test
    public void applicationManagedModeDoesNotCreateStarterFile() throws Exception {
        Path directory = Files.createTempDirectory("druid-logback-managed-");
        DruidLogbackMetricsEventSink sink = null;
        try {
            DruidStatProperties.Prometheus config = config(directory, "events.log");
            config.getLogging().setAutoConfigure(false);
            sink = new DruidLogbackMetricsEventSink(config);
            sink.init();

            assertTrue(sink.isActive());
            assertFalse(Files.exists(directory.resolve("events.log")));
        } finally {
            if (sink != null) sink.close();
            delete(directory);
        }
    }

    @Test
    public void invalidRefreshKeepsPreviousAppender() throws Exception {
        Path directory = Files.createTempDirectory("druid-logback-rollback-");
        Path invalidDirectory = Files.createTempFile("druid-logback-not-directory-", ".tmp");
        DruidLogbackMetricsEventSink sink = null;
        try {
            DruidStatProperties.Prometheus oldConfig = config(directory, "events.log");
            sink = new DruidLogbackMetricsEventSink(oldConfig);
            sink.init();
            DruidStatProperties.Prometheus invalidConfig = config(invalidDirectory, "events.log");

            assertFalse(sink.refresh(oldConfig, invalidConfig));
            sink.log("{\"after_failed_refresh\":true}", true);
            sink.close();
            sink = null;

            assertTrue(read(directory.resolve("events.log")).contains("{\"after_failed_refresh\":true}"));
        } finally {
            if (sink != null) sink.close();
            delete(directory);
            Files.deleteIfExists(invalidDirectory);
        }
    }

    private static DruidStatProperties.Prometheus config(Path directory, String fileName) {
        DruidStatProperties.Prometheus config = new DruidStatProperties.Prometheus();
        config.getLogging().setDirectory(directory.toString());
        config.getLogging().setFileName(fileName);
        config.getLogging().setQueueSize(16);
        return config;
    }

    private static String read(Path file) throws Exception {
        assertTrue(Files.exists(file));
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }

    private static void delete(Path path) throws Exception {
        if (!Files.exists(path)) return;
        if (Files.isDirectory(path)) {
            java.nio.file.DirectoryStream<Path> children = Files.newDirectoryStream(path);
            try {
                for (Path child : children) delete(child);
            } finally {
                children.close();
            }
        }
        Files.deleteIfExists(path);
    }
}
