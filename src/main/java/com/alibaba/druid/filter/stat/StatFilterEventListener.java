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
package com.alibaba.druid.filter.stat;

import com.alibaba.druid.proxy.jdbc.DataSourceProxy;

/**
 * Receives completed JDBC statistic events without depending on a metrics library.
 *
 * <p>Implementations must return promptly. Exceptions are isolated by
 * {@link StatFilterContext} and never affect JDBC execution.</p>
 */
public interface StatFilterEventListener {
    void onSqlExecute(String sql, DataSourceProxy dataSource, long durationNanos, Throwable error);

    void onSqlUpdateCount(String sql, DataSourceProxy dataSource, int updateCount);

    void onSqlResultSetClose(String sql, DataSourceProxy dataSource, int fetchRowCount);
}
