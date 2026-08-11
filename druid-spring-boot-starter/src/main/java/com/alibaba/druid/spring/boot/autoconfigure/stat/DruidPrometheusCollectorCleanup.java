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

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 清理 Micrometer Prometheus 1.1.x 暴露层中已经从 MeterRegistry 移除的明细序列。
 *
 * <p>Micrometer 1.1.0 的 {@code PrometheusMeterRegistry} 将每个 Meter 的采样器
 * 保存在内部 {@code MicrometerCollector.children} 中，但 MeterRegistry.remove()
 * 没有同步移除该采样器。因此这里优先使用 Micrometer/Prometheus 的公开 API，
 * 仅在需要定位 collector child 时使用版本相关反射。反射失败时记录 WARN/ERROR，
 * 不影响 SQL/URI 事件主路径。</p>
 *
 * <p><strong>REMARK：</strong>当前固定部署由应用直接注入绑定
 * {@code CollectorRegistry.defaultRegistry} 的 {@code PrometheusMeterRegistry}，
 * 本适配器不再递归兼容 {@link CompositeMeterRegistry}。若未来 Registry 结构变化，
 * 必须重新设计暴露层清理并通过真实 Prometheus endpoint E2E 后再启用。</p>
 */
final class DruidPrometheusCollectorCleanup {
    private static final Logger LOG = LoggerFactory.getLogger("druid.prometheus.detail");
    private static final String SQL_PREFIX = "druid_sql_";
    private static final String URI_PREFIX = "druid_uri_";
    /** 一期固定的 7 个明细 Meter；周期清理不依赖本地状态或 Registry 快照推导 family。 */
    private static final List<Meter.Id> DETAIL_METER_IDS;

    static {
        List<Meter.Id> ids = new ArrayList<Meter.Id>(7);
        ids.add(detailId(DruidPrometheusMetricsListener.SQL_EXECUTION_DURATION, Meter.Type.TIMER));
        ids.add(detailId(DruidPrometheusMetricsListener.SQL_AFFECTED_ROWS, Meter.Type.DISTRIBUTION_SUMMARY));
        ids.add(detailId(DruidPrometheusMetricsListener.SQL_FETCHED_ROWS, Meter.Type.DISTRIBUTION_SUMMARY));
        ids.add(detailId(DruidPrometheusMetricsListener.URI_REQUEST_DURATION, Meter.Type.TIMER));
        ids.add(detailId(DruidPrometheusMetricsListener.URI_JDBC_EXECUTIONS, Meter.Type.DISTRIBUTION_SUMMARY));
        ids.add(detailId(DruidPrometheusMetricsListener.URI_JDBC_AFFECTED_ROWS, Meter.Type.DISTRIBUTION_SUMMARY));
        ids.add(detailId(DruidPrometheusMetricsListener.URI_JDBC_FETCHED_ROWS, Meter.Type.DISTRIBUTION_SUMMARY));
        DETAIL_METER_IDS = Collections.unmodifiableList(ids);
    }

    private final MeterRegistry meterRegistry;
    private volatile boolean bindingLogged;

    /** 创建绑定到应用实际 MeterRegistry 的 Prometheus 暴露层清理适配器。 */
    DruidPrometheusCollectorCleanup(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /** 输出 Registry 类型及当前 Prometheus Druid metric family，便于确认导出链路。 */
    void logBinding(String phase) {
        if (!LOG.isTraceEnabled()) {
            return;
        }
        try {
            Object collectorRegistry = directPrometheusCollectorRegistry(meterRegistry);
            Object defaultCollectorRegistry = simpleclientDefaultCollectorRegistry();
            if (collectorRegistry == null) {
                if (!bindingLogged || "cleanup".equals(phase)) {
                    LOG.trace("druid prometheus registry: phase={} meterRegistryType={} "
                                    + "meterRegistryIdentity={} collectorRegistryType=unavailable "
                                    + "simpleclientDefaultIdentity={} defaultDruidFamilies={} "
                                    + "detail={}",
                            phase, meterRegistry.getClass().getName(), System.identityHashCode(meterRegistry),
                            identity(defaultCollectorRegistry), druidFamiliesOrEmpty(defaultCollectorRegistry),
                            meterRegistry instanceof CompositeMeterRegistry
                                    ? "unsupported-composite-registry" : "not-a-prometheus-registry");
                    bindingLogged = true;
                }
                return;
            }
            List<String> families = druidFamilies(collectorRegistry);
            LOG.trace("druid prometheus registry: phase={} meterRegistryType={} "
                            + "meterRegistryIdentity={} collectorRegistryType={} "
                            + "collectorRegistryIdentity={} sharedWithSimpleclientDefault={} "
                            + "druidFamilies={} defaultDruidFamilies={}",
                    phase, meterRegistry.getClass().getName(), System.identityHashCode(meterRegistry),
                    collectorRegistry.getClass().getName(), System.identityHashCode(collectorRegistry),
                    collectorRegistry == defaultCollectorRegistry, families,
                    collectorRegistry == defaultCollectorRegistry
                            ? families : druidFamiliesOrEmpty(defaultCollectorRegistry));
            bindingLogged = true;
        } catch (Exception error) {
            LOG.warn("druid prometheus registry diagnostics failed: phase={} meterRegistryType={} error={}",
                    phase, meterRegistry.getClass().getName(), error.toString());
        }
    }

    /**
     * 从 Prometheus 暴露层删除单个 Druid Meter 对应的 child。只处理 Druid SQL/URI family。
     * 返回值表示是否已完成暴露层同步；false 时 MeterRegistry 中的删除仍然有效。
     */
    boolean remove(Meter meter) {
        if (meter == null || !isDruidMeter(meter.getId().getName())) {
            return true;
        }
        try {
            if (directPrometheusCollectorRegistry(meterRegistry) == null) {
                // REMARK：Composite 不做递归兼容；返回失败交由上层限频告警，防止静默残留。
                return !(meterRegistry instanceof CompositeMeterRegistry);
            }
            String familyName = conventionName(meterRegistry, meter.getId());
            Object collector = collectorFor(meterRegistry, familyName);
            if (collector == null) {
                return true;
            }
            boolean removed = removeChild(collector, meter);
            if (!removed) {
                // 空 collector 不会输出 metric family；重复清理时允许幂等成功。
                if (childrenOf(collector).isEmpty()) {
                    return true;
                }
                LOG.warn("druid prometheus collector child not found: family={} meter={} "
                                + "micrometerVersionRisk=1.1.x-internal-layout",
                        familyName, meter.getId());
                return false;
            }
            // 空 collector collect() 返回空列表，保留它可供后续同名 Meter 重新注册。
            return true;
        } catch (Exception error) {
            LOG.warn("druid prometheus collector cleanup failed: family={} meter={} "
                            + "micrometerVersionRisk=reflection-layout-changed error={}",
                    conventionName(meterRegistry, meter.getId()), meter.getId(), error.toString());
            return false;
        }
    }

    /** 周期清理固定清空一期 7 个 Prometheus family 的全部 child。 */
    boolean clearAllDetailChildren() {
        try {
            if (directPrometheusCollectorRegistry(meterRegistry) == null) {
                if (meterRegistry instanceof CompositeMeterRegistry) {
                    // REMARK：Registry 结构变化必须先补充新方案及 endpoint E2E，不允许静默跳过。
                    LOG.error("druid prometheus collector family cleanup aborted: "
                                    + "unsupportedRegistryType={} expected=direct-PrometheusMeterRegistry",
                            meterRegistry.getClass().getName());
                    return false;
                }
                // Simple/JMX 等 Registry 没有 Prometheus 暴露层，只需清理 Micrometer Meter。
                return true;
            }

            List<Collection<?>> children = new ArrayList<Collection<?>>(DETAIL_METER_IDS.size());
            for (Meter.Id id : DETAIL_METER_IDS) {
                String family = conventionName(meterRegistry, id);
                Object collector = collectorFor(meterRegistry, family);
                if (collector != null) {
                    children.add(childrenOf(collector));
                }
            }
            // 先完成全部反射解析，再统一修改，避免解析中途失败造成部分 family 已清空。
            for (Collection<?> familyChildren : children) {
                familyChildren.clear();
            }
            return true;
        } catch (Exception error) {
            LOG.error("druid prometheus collector family cleanup failed; periodic cleanup aborted: "
                            + "meterRegistryType={} "
                            + "micrometerVersionRisk=reflection-layout-changed error={}",
                    meterRegistry.getClass().getName(), error.toString());
            return false;
        }
    }

    /** 构造不带标签的固定明细 Meter ID，仅用于按 NamingConvention 定位 family。 */
    private static Meter.Id detailId(String name, Meter.Type type) {
        return new Meter.Id(name, Tags.empty(), null, null, type);
    }

    /**
     * 取得直接 PrometheusMeterRegistry 绑定的 CollectorRegistry。
     * 返回 null 表示不是直接 Prometheus Registry；这里不会向 Composite 子 Registry 递归。
     */
    private Object directPrometheusCollectorRegistry(MeterRegistry registry) throws Exception {
        Method method = findMethod(registry.getClass(), "getPrometheusRegistry");
        return method == null ? null : method.invoke(registry);
    }

    /** simpleclient_spring_boot 的 PrometheusEndpoint 固定暴露 CollectorRegistry.defaultRegistry。 */
    private Object simpleclientDefaultCollectorRegistry() {
        try {
            ClassLoader loader = meterRegistry.getClass().getClassLoader();
            Class<?> type = Class.forName("io.prometheus.client.CollectorRegistry", false, loader);
            return type.getField("defaultRegistry").get(null);
        } catch (Exception ignored) {
            return null;
        }
    }

    /** 诊断场景下安全读取 Druid family；依赖缺失或布局变化时返回空集合。 */
    private List<String> druidFamiliesOrEmpty(Object collectorRegistry) {
        if (collectorRegistry == null) {
            return Collections.emptyList();
        }
        try {
            return druidFamilies(collectorRegistry);
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
    }

    /** 输出对象 identity，空对象使用可读占位符，便于对比两个 CollectorRegistry。 */
    private static Object identity(Object value) {
        return value == null ? "unavailable" : System.identityHashCode(value);
    }

    /** 按 Micrometer 计算出的 family 名从 1.1.x collectorMap 中取得 collector。 */
    private Object collectorFor(MeterRegistry registry, String familyName) throws Exception {
        Field field = findField(registry.getClass(), "collectorMap");
        if (field == null) {
            throw new NoSuchFieldException(registry.getClass().getName() + ".collectorMap");
        }
        field.setAccessible(true);
        Object value = field.get(registry);
        if (!(value instanceof Map)) {
            throw new IllegalStateException("collectorMap is not a Map: " + value);
        }
        return ((Map<?, ?>) value).get(familyName);
    }

    /** 从指定 family 精确删除引用目标 Meter 的一个 child，不影响同 family 的其他标签。 */
    private boolean removeChild(Object collector, Meter meter) throws Exception {
        Collection<?> children = childrenOf(collector);
        Iterator<?> iterator = children.iterator();
        while (iterator.hasNext()) {
            Object child = iterator.next();
            if (referencesMeter(child, meter)) {
                if (children instanceof java.util.concurrent.CopyOnWriteArrayList) {
                    return ((java.util.concurrent.CopyOnWriteArrayList<?>) children).remove(child);
                }
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    /** 读取 Micrometer 1.1.x MicrometerCollector.children 集合。 */
    private Collection<?> childrenOf(Object collector) throws Exception {
        Field field = findField(collector.getClass(), "children");
        if (field == null) {
            throw new NoSuchFieldException(collector.getClass().getName() + ".children");
        }
        field.setAccessible(true);
        Object value = field.get(collector);
        if (!(value instanceof Collection)) {
            throw new IllegalStateException("collector children is not a Collection: " + value);
        }
        return (Collection<?>) value;
    }

    /**
     * 判断 collector child 是否捕获目标 Meter。
     * 只检查 child 自身及其父类声明字段，不递归遍历任意对象图。
     */
    private boolean referencesMeter(Object value, Meter meter) throws IllegalAccessException {
        if (value == meter) {
            return true;
        }
        if (value == null) {
            return false;
        }
        Class<?> type = value.getClass();
        while (type != null && type != Object.class) {
            for (Field field : type.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                Object nested = field.get(value);
                if (nested == meter || (nested instanceof Meter
                        && ((Meter) nested).getId().equals(meter.getId()))) {
                    return true;
                }
            }
            type = type.getSuperclass();
        }
        return false;
    }

    /** 枚举 CollectorRegistry 当前实际输出的 Druid SQL/URI metric family。 */
    private List<String> druidFamilies(Object collectorRegistry) throws Exception {
        Method method = findMethod(collectorRegistry.getClass(), "metricFamilySamples");
        if (method == null) {
            return Collections.emptyList();
        }
        Object result = method.invoke(collectorRegistry);
        if (!(result instanceof Enumeration)) {
            return Collections.emptyList();
        }
        List<String> families = new ArrayList<String>();
        Enumeration<?> enumeration = (Enumeration<?>) result;
        while (enumeration.hasMoreElements()) {
            Object family = enumeration.nextElement();
            Field name = findField(family.getClass(), "name");
            if (name != null) {
                name.setAccessible(true);
                Object value = name.get(family);
                if (value instanceof String && isDruidFamily((String) value)) {
                    families.add((String) value);
                }
            }
        }
        Collections.sort(families);
        return families;
    }

    /** 判断 Micrometer 逻辑名称是否属于 Druid SQL/URI 指标命名空间。 */
    private static boolean isDruidMeter(String name) {
        return name != null && (name.startsWith("druid.sql.") || name.startsWith("druid.uri."));
    }

    /** 判断 Prometheus family 名称是否属于 Druid SQL/URI 指标命名空间。 */
    private static boolean isDruidFamily(String name) {
        return name != null && (name.startsWith(SQL_PREFIX) || name.startsWith(URI_PREFIX));
    }

    /** 使用 Registry 当前 NamingConvention 将逻辑 Meter ID 转换为 collectorMap key。 */
    private static String conventionName(MeterRegistry registry, Meter.Id id) {
        return id.getConventionName(registry.config().namingConvention());
    }

    /** 优先查找公开方法，旧版本无公开 API 时再沿类层级查找声明方法。 */
    private static Method findMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        // 公开 API 优先，只有旧版本没有公开方法时才继续查找声明方法。
        try {
            return type.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            // 继续尝试版本相关的声明方法
        }
        Class<?> current = type;
        while (current != null) {
            try {
                Method method = current.getDeclaredMethod(name, parameterTypes);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    /** 优先查找公开字段，旧版本内部布局字段再沿类层级查找。 */
    private static Field findField(Class<?> type, String name) {
        // 诊断字段等公开 API 优先；collectorMap/children 才会进入私有字段反射。
        try {
            return type.getField(name);
        } catch (NoSuchFieldException ignored) {
            // 继续尝试版本相关的声明字段
        }
        Class<?> current = type;
        while (current != null) {
            try {
                return current.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        return null;
    }
}
