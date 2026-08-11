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

/** 手动清理 Druid Prometheus SQL/URI 明细指标的不可变结果。 */
public final class DruidPrometheusCleanupResult {
    private final boolean executed;
    private final boolean familySuccess;
    private final String detail;
    private final int sqlIdentityBefore;
    private final int uriIdentityBefore;
    private final int sqlMeterBefore;
    private final int uriMeterBefore;
    private final int sqlMeterSuccess;
    private final int sqlMeterFailed;
    private final int uriMeterSuccess;
    private final int uriMeterFailed;

    /** 创建一次手动清理结果；仅由 Starter 内部清理实现调用。 */
    DruidPrometheusCleanupResult(boolean executed, boolean familySuccess, String detail,
                                 int sqlIdentityBefore, int uriIdentityBefore,
                                 int sqlMeterBefore, int uriMeterBefore,
                                 int sqlMeterSuccess, int sqlMeterFailed,
                                 int uriMeterSuccess, int uriMeterFailed) {
        this.executed = executed;
        this.familySuccess = familySuccess;
        this.detail = detail;
        this.sqlIdentityBefore = sqlIdentityBefore;
        this.uriIdentityBefore = uriIdentityBefore;
        this.sqlMeterBefore = sqlMeterBefore;
        this.uriMeterBefore = uriMeterBefore;
        this.sqlMeterSuccess = sqlMeterSuccess;
        this.sqlMeterFailed = sqlMeterFailed;
        this.uriMeterSuccess = uriMeterSuccess;
        this.uriMeterFailed = uriMeterFailed;
    }

    /** 返回是否实际进入清理链路；Listener 尚未初始化时为 false。 */
    public boolean isExecuted() {
        return executed;
    }

    /** 返回 Prometheus family 暴露层是否清理成功。 */
    public boolean isFamilySuccess() {
        return familySuccess;
    }

    /** 返回便于调用方诊断的稳定结果说明。 */
    public String getDetail() {
        return detail;
    }

    /** 返回清理前 SQL identity 数。 */
    public int getSqlIdentityBefore() {
        return sqlIdentityBefore;
    }

    /** 返回清理前 URI identity 数。 */
    public int getUriIdentityBefore() {
        return uriIdentityBefore;
    }

    /** 返回清理前 SQL Meter 数。 */
    public int getSqlMeterBefore() {
        return sqlMeterBefore;
    }

    /** 返回清理前 URI Meter 数。 */
    public int getUriMeterBefore() {
        return uriMeterBefore;
    }

    /** 返回成功注销的 SQL Meter 数。 */
    public int getSqlMeterSuccess() {
        return sqlMeterSuccess;
    }

    /** 返回注销失败的 SQL Meter 数。 */
    public int getSqlMeterFailed() {
        return sqlMeterFailed;
    }

    /** 返回成功注销的 URI Meter 数。 */
    public int getUriMeterSuccess() {
        return uriMeterSuccess;
    }

    /** 返回注销失败的 URI Meter 数。 */
    public int getUriMeterFailed() {
        return uriMeterFailed;
    }
}
