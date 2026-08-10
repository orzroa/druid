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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 清理 Micrometer Prometheus 1.1.x 暴露层中已经从 MeterRegistry 移除的明细序列。
 *
 * <p>Micrometer 1.1.0 的 {@code PrometheusMeterRegistry} 将每个 Meter 的采样器
 * 保存在内部 {@code MicrometerCollector.children} 中，但 MeterRegistry.remove()
 * 没有同步移除该采样器。因此这里优先使用 Micrometer/Prometheus 的公开 API，
 * 仅在需要定位 collector child 时使用版本相关反射。反射失败时只记录告警，
 * 不影响 SQL/URI 事件主路径。</p>
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

    DruidPrometheusCollectorCleanup(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /** 输出 Registry 类型及当前 Prometheus Druid metric family，便于确认导出链路。 */
    void logBinding(String phase) {
        if (!LOG.isTraceEnabled()) {
            return;
        }
        try {
            Object collectorRegistry = prometheusCollectorRegistry();
            Object defaultCollectorRegistry = simpleclientDefaultCollectorRegistry();
            if (collectorRegistry == null) {
                if (!bindingLogged || "cleanup".equals(phase)) {
                    LOG.trace("druid prometheus registry: phase={} meterRegistryType={} "
                                    + "meterRegistryIdentity={} collectorRegistryType=unavailable "
                                    + "simpleclientDefaultIdentity={} defaultDruidFamilies={} "
                                    + "detail=not-a-prometheus-registry",
                            phase, meterRegistry.getClass().getName(), System.identityHashCode(meterRegistry),
                            identity(defaultCollectorRegistry), druidFamiliesOrEmpty(defaultCollectorRegistry));
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
            // 直接 Registry 使用调用方持有的 Meter，避免 LRU 热路径在 remove() 后再 O(N) 扫描。
            return removeFromRegistry(meterRegistry, meter, false);
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
            Set<Collection<?>> children = Collections.newSetFromMap(
                    new IdentityHashMap<Collection<?>, Boolean>());
            collectChildren(meterRegistry, DETAIL_METER_IDS, children);
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

    /**
     * CompositeMeterRegistry.remove() 不保证注销所有子 Registry 的实际 Meter。
     * family child 清空后通过 Micrometer 公开 API 扫描一次子 Registry 并同步注销，
     * 避免后续注册复用一个已失去 Prometheus child 的旧 Meter。
     */
    boolean removeDetailMetersFromCompositeChildren() {
        if (!(meterRegistry instanceof CompositeMeterRegistry)) {
            return true;
        }
        try {
            Set<MeterRegistry> visited = Collections.newSetFromMap(
                    new IdentityHashMap<MeterRegistry, Boolean>());
            for (MeterRegistry child : ((CompositeMeterRegistry) meterRegistry).getRegistries()) {
                removeDetailMeters(child, visited);
            }
            return true;
        } catch (RuntimeException error) {
            LOG.error("druid micrometer composite child cleanup failed: meterRegistryType={} error={}",
                    meterRegistry.getClass().getName(), error.toString());
            return false;
        }
    }

    private static Meter.Id detailId(String name, Meter.Type type) {
        return new Meter.Id(name, Tags.empty(), null, null, type);
    }

    private void collectChildren(MeterRegistry registry, List<Meter.Id> ids,
                                 Set<Collection<?>> children) throws Exception {
        if (directPrometheusCollectorRegistry(registry) != null) {
            Set<String> families = new HashSet<String>();
            for (Meter.Id id : ids) {
                families.add(conventionName(registry, id));
            }
            for (String family : families) {
                Object collector = collectorFor(registry, family);
                if (collector != null) {
                    children.add(childrenOf(collector));
                }
            }
            return;
        }

        if (!(registry instanceof CompositeMeterRegistry)) {
            return;
        }
        for (MeterRegistry child : ((CompositeMeterRegistry) registry).getRegistries()) {
            collectChildren(child, ids, children);
        }
    }

    private boolean removeFromRegistry(MeterRegistry registry, Meter target,
                                       boolean locateChildMeter) throws Exception {
        Object collectorRegistry = directPrometheusCollectorRegistry(registry);
        if (collectorRegistry != null) {
            Meter actual = target;
            if (locateChildMeter) {
                actual = findMeter(registry, target);
                if (actual == null) {
                    // 子 Registry 已经不再持有 Meter 时，暴露层 child 仍可能存在；用目标 ID 继续定位。
                    actual = target;
                }
            }
            // collectorMap 的 key 由 Registry 的 NamingConvention 生成，不能手写下划线规则。
            String familyName = conventionName(registry, actual.getId());
            Object collector = collectorFor(registry, familyName);
            if (collector == null) {
                return true;
            }
            // CompositeMeterRegistry 的 remove 不会移除子 Registry 中的实际 Meter，先通过公开 API 移除。
            if (locateChildMeter && actual != target) {
                registry.remove(actual);
            }
            boolean removed = removeChild(collector, actual);
            if (!removed) {
                // 空 collector 不会输出 metric family；重复清理时允许幂等成功。
                if (childrenOf(collector).isEmpty()) {
                    return true;
                }
                LOG.warn("druid prometheus collector child not found: family={} meter={} "
                                + "micrometerVersionRisk=1.1.x-internal-layout",
                        familyName, actual.getId());
                return false;
            }
            // MicrometerCollector 没有 child 时 collect() 返回空列表，无需注销整个 family。
            // 保留空 collector 还能让同名 Meter 后续直接复用，避免修改 collectorMap 私有状态。
            return true;
        }

        if (!(registry instanceof CompositeMeterRegistry)) {
            // 普通 Simple/JMX 等 Registry 没有 Prometheus 暴露层，不需要额外同步。
            return true;
        }
        boolean found = false;
        boolean success = true;
        for (MeterRegistry child : ((CompositeMeterRegistry) registry).getRegistries()) {
            if (directPrometheusCollectorRegistry(child) != null
                    || child instanceof CompositeMeterRegistry) {
                found = true;
                success &= removeFromRegistry(child, target, true);
            }
        }
        return !found || success;
    }

    private void removeDetailMeters(MeterRegistry registry, Set<MeterRegistry> visited) {
        if (!visited.add(registry)) {
            return;
        }
        // getMeters() 只扫描一次并复制，避免边遍历边注销引发实现相关行为。
        for (Meter meter : new ArrayList<Meter>(registry.getMeters())) {
            if (isDetailMeter(meter.getId().getName())) {
                registry.remove(meter);
            }
        }
        if (registry instanceof CompositeMeterRegistry) {
            for (MeterRegistry child : ((CompositeMeterRegistry) registry).getRegistries()) {
                removeDetailMeters(child, visited);
            }
        }
    }

    private Object prometheusCollectorRegistry() throws Exception {
        Object direct = directPrometheusCollectorRegistry(meterRegistry);
        if (direct != null) {
            return direct;
        }
        if (!(meterRegistry instanceof CompositeMeterRegistry)) {
            return null;
        }
        for (MeterRegistry child : ((CompositeMeterRegistry) meterRegistry).getRegistries()) {
            Object nested = new DruidPrometheusCollectorCleanup(child).prometheusCollectorRegistry();
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

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

    private static Object identity(Object value) {
        return value == null ? "unavailable" : System.identityHashCode(value);
    }

    private Meter findMeter(MeterRegistry registry, Meter target) {
        for (Meter meter : registry.getMeters()) {
            if (meter.getId().equals(target.getId())) {
                return meter;
            }
        }
        return null;
    }

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

    private static boolean isDruidMeter(String name) {
        return name != null && (name.startsWith("druid.sql.") || name.startsWith("druid.uri."));
    }

    private static boolean isDetailMeter(String name) {
        for (Meter.Id id : DETAIL_METER_IDS) {
            if (id.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDruidFamily(String name) {
        return name != null && (name.startsWith(SQL_PREFIX) || name.startsWith(URI_PREFIX));
    }

    private static String conventionName(MeterRegistry registry, Meter.Id id) {
        return id.getConventionName(registry.config().namingConvention());
    }

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
