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
package com.alibaba.druid.support.http;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Global, dependency-free WebStat event dispatcher.
 */
public final class WebStatEventContext {
    private static final List<WebStatEventListener> LISTENERS = new CopyOnWriteArrayList<WebStatEventListener>();

    private WebStatEventContext() {
    }

    public static void addListener(WebStatEventListener listener) {
        if (listener != null) {
            LISTENERS.add(listener);
        }
    }

    public static void removeListener(WebStatEventListener listener) {
        LISTENERS.remove(listener);
    }

    public static boolean hasListeners() {
        return !LISTENERS.isEmpty();
    }

    public static void fireWebRequest(HttpServletRequest request, String uri, long durationNanos,
                                      long jdbcExecuteCount, long jdbcUpdateCount,
                                      long jdbcFetchRowCount, Throwable error) {
        for (WebStatEventListener listener : LISTENERS) {
            try {
                listener.onWebRequest(request, uri, durationNanos, jdbcExecuteCount,
                        jdbcUpdateCount, jdbcFetchRowCount, error);
            } catch (Throwable ignored) {
                // Metrics listeners must never affect application requests.
            }
        }
    }
}
