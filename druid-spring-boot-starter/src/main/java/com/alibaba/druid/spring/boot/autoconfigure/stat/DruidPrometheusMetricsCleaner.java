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

/**
 * Druid Prometheus 一期 SQL/URI 明细指标的手动清理接口。
 *
 * <p>实现必须复用自动清理的互斥和删除链路，但手动调用不得改变自动清理的时间基准或
 * 下一次调度边界。HTTP、鉴权和环境限制由集成应用自行决定。</p>
 */
public interface DruidPrometheusMetricsCleaner {
    /** 立即清理当前应用实例中的 Druid SQL/URI 明细指标并返回执行结果。 */
    DruidPrometheusCleanupResult cleanupNow();
}
