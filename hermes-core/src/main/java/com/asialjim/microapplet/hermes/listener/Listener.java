/*
 *    Copyright 2014-2026 <a href="mailto:asialjim@qq.com">Asial Jim</a>
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.asialjim.microapplet.hermes.listener;

import com.asialjim.microapplet.hermes.HermesService;
import com.asialjim.microapplet.hermes.event.EventBus;
import com.asialjim.microapplet.hermes.event.Hermes;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 顶级接口，声明为监听器
 * Top-level interface, declared as a listener
 *
 * @param <E> 事件数据类型
 *            Event data type
 * @author <a href="mailto:asialjim@qq.com">Asial Jim</a>
 * @version 1.0.0
 * @since 2026-01-08
 */
public interface Listener<E> extends EventListener, Comparable<Listener<E>> {
    /**
     * 获取日志记录器
     * Get logger instance
     *
     * @return 日志记录器实例
     * Logger instance
     * @since 2026-01-08
     */
    default Logger log() {
        return LoggerFactory.getLogger(this.getClass());
    }

    /**
     * 获取监听器的执行顺序
     * Get the execution order of the listener
     *
     * @return 执行顺序，值越小优先级越高
     * Execution order, smaller value means higher priority
     * @since 2026-01-08
     */
    default int getOrder() {
        return 0;
    }

    /**
     * 比较两个监听器的执行顺序
     * Compare the execution order of two listeners
     *
     * @param o 另一个监听器实例
     *          Another listener instance
     * @return 比较结果，负数表示当前监听器优先级更高
     * Comparison result, negative number means current listener has higher priority
     * @since 2026-01-08
     */
    @Override
    default int compareTo(Listener<E> o) {
        return this.getOrder() - o.getOrder();
    }

    /**
     * 监听器所属服务名称
     * Service name that the listener belongs to
     *
     * @return 服务名称实例
     * Service name instance
     * @since 2026-01-08
     */
    HermesService getServiceName();

    /**
     * 监听器注册到本地事件总线
     * Register listener to local event bus
     *
     * @since 2026-01-08
     */
    @PostConstruct
    default void register() {
        EventBus.register(this);
    }

    /**
     * 监听事件
     * Listen to event
     *
     * @param event 事件对象
     *              Event object
     * @since 2026-01-08
     */
    default void onEvent(E event) {
        onEvent(null,event);
    }

    /**
     * 监听带事件编号的事件,用于事件溯源
     * Listen to event with ID, used for event tracing
     *
     * @param id    事件ID
     *              Event ID
     * @param event 事件对象
     *              Event object
     * @since 2026-01-08
     */
    default void onEvent(String id, E event) {
        onEventAfter(id, event, null);
    }


    default void onEventAfter(E event, Duration after) {
        onEventAfter(null, event, after);
    }

    default void onEventAfter(String id, E event, Duration after) {
        StopWatch stopWatch = new StopWatch();
        Hermes<E> hermes = wrapHermes(event).setId(id).setTrigAfter(after);
        onHermes(event, stopWatch, hermes);
    }


    /**
     * 将事件包装为 Hermes
     * Wrap event as Hermes
     *
     * @param event 事件对象
     *              Event object
     * @return 包装后的Hermes事件
     * Wrapped Hermes event
     * @since 2026-01-08
     */
    private Hermes<E> wrapHermes(E event) {
        if (event instanceof Hermes<?> hermes)
            //noinspection unchecked
            return (Hermes<E>) hermes;

        // 默认包装为全局事件
        return new Hermes<E>()
                .setGlobal(true)
                .setSendTime(LocalDateTime.now())
                .setType(event.getClass().getTypeName())
                .setData(event);
    }

    /**
     * 监听器处理 Hermes
     * Listener processes Hermes
     *
     * @param event     原始事件对象
     *                  Original event object
     * @param stopWatch 计时器，用于统计处理时间
     *                  Stopwatch for measuring processing time
     * @param hermes    包装后的Hermes事件
     *                  Wrapped Hermes event
     * @since 2026-01-08
     */
    private void onHermes(E event, StopWatch stopWatch, Hermes<E> hermes) {
        try {
            stopWatch.start();
            if (log().isDebugEnabled())
                log().info("监听事件[{}]处理进入...", event);
            before(hermes);
            if (log().isDebugEnabled())
                log().info("监听事件[{}]处理开始...", event);
            doOnEvent(hermes);
            if (log().isDebugEnabled())
                log().info("监听事件[{}]处理结束...", event);
            stopWatch.stop();
            long time = stopWatch.getTime(TimeUnit.MILLISECONDS);
            if (log().isDebugEnabled())
                log().info("监听事件[{}]处理耗时[{} 毫秒]", event, time);
            onAfter(hermes);
        } catch (Throwable e) {
            if (log().isDebugEnabled()) log().error("监听事件：{},异常:{}", event, e.getMessage(), e);
            else log().info("监听事件：{},异常:{}", event, e.getMessage());

            stopWatch.stop();
            stopWatch.reset();

            stopWatch.start();
            onError(hermes, e);
            stopWatch.stop();

            long time = stopWatch.getTime(TimeUnit.MILLISECONDS);
            if (log().isDebugEnabled())
                log().info("监听事件[{}]异常回调耗时[{} 毫秒]", event, time);
        } finally {
            onFinal(hermes);
        }
    }

    /**
     * 事件处理前的回调方法
     * Callback method before event processing
     *
     * @param ignoredEvent 包装后的Hermes事件
     *                     Wrapped Hermes event
     * @since 2026-01-08
     */
    default void before(Hermes<E> ignoredEvent) {
        // do nothing here
    }

    /**
     * 执行事件处理的核心方法
     * Core method for event processing
     *
     * @param event 包装后的Hermes事件
     *              Wrapped Hermes event
     * @throws Throwable 事件处理过程中可能抛出的异常
     *                   Exception that may be thrown during event processing
     * @since 2026-01-08
     */
    void doOnEvent(Hermes<E> event) throws Throwable;

    /**
     * 事件处理异常时的回调方法
     * Callback method when event processing fails
     *
     * @param ignoredEvent 包装后的Hermes事件
     *                     Wrapped Hermes event
     * @param ignoredEx    事件处理过程中抛出的异常
     *                     Exception thrown during event processing
     * @since 2026-01-08
     */
    default void onError(Hermes<E> ignoredEvent, Throwable ignoredEx) {
        // do nothing here default
    }

    /**
     * 事件处理最终的回调方法，无论成功或失败都会执行
     * Final callback method for event processing, executed regardless of success or failure
     *
     * @param ignoredEvent 包装后的Hermes事件
     *                     Wrapped Hermes event
     * @since 2026-01-08
     */
    default void onFinal(Hermes<E> ignoredEvent) {
        // do nothing here default
    }

    /**
     * 事件处理成功后的回调方法
     * Callback method after successful event processing
     *
     * @param event 包装后的Hermes事件
     *              Wrapped Hermes event
     * @since 2026-01-08
     */
    default void onAfter(Hermes<E> event) {
        // do nothing here default
    }

    /**
     * 获取当前监听器感兴趣的事件类型集合
     * Get the set of event types that the current listener is interested in
     *
     * @return 事件类型集合
     * Set of event types
     * @since 2026-01-08
     */
    default Set<Type> eventType() {
        Set<Type> res = new HashSet<>();
        Class<?> beanClass = this.getClass();
        Set<Type> genericInterfaces = new HashSet<>();
        genericInterfaces(genericInterfaces, beanClass);
        for (Type type : genericInterfaces) {
            addType(type, res);
        }
        return res;
    }

    /**
     * 是否是全局监听器
     * Whether it is a global listener
     * <pre>
     * 返回{@code true}时，该监听器将会处理任何类型的事件
     * 返回{@code false}时，该监听器只会处理 {@link #eventType()}集合类型内规定的事件
     * 默认判定标准为：{
     *     {@link #eventType()} 中是否有能够处理任何{@link Object}事件的能力
     * }
     * When returning {@code true}, the listener will process any type of event
     * When returning {@code false}, the listener will only process events within the {@link #eventType()} set
     * Default judgment criterion: {
     *     Whether there is ability to handle any {@link Object} event in {@link #eventType()}
     * }
     * </pre>
     *
     * @return 是否为全局监听器
     * Whether it is a global listener
     * @since 2026-01-08
     */
    default boolean globalListener() {
        Set<Type> types = eventType();
        if (Objects.isNull(types))
            return false;
        String objectType = Object.class.getTypeName();
        return types.stream()
                .map(Type::getTypeName)
                .anyMatch(item -> StringUtils.equals(objectType, item));
    }

    /**
     * 将符合条件的类型添加到结果集合中
     * Add qualified types to the result set
     *
     * @param type 类型对象
     *             Type object
     * @param res  结果集合
     *             Result set
     * @since 2026-01-08
     */
    private static void addType(Type type, Set<Type> res) {
        if (Objects.isNull(type))
            return;

        if (!(type instanceof ParameterizedType parameterizedType))
            return;

        Type rawType = parameterizedType.getRawType();
        if (!candidateType(rawType))
            return;

        // 获取接口的泛型参数
        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        if (ArrayUtils.isEmpty(actualTypeArguments))
            return;

        Collections.addAll(res, actualTypeArguments);
    }

    /**
     * 检查类型是否为Listener接口或其子接口
     * Check if the type is Listener interface or its subinterface
     *
     * @param rawType 原始类型对象
     *                Raw type object
     * @return 是否为Listener类型
     * Whether it is a Listener type
     * @since 2026-01-08
     */
    private static boolean candidateType(Type rawType) {
        if (Objects.isNull(rawType))
            return false;
        String typeName = rawType.getTypeName();
        if (StringUtils.isBlank(typeName))
            return false;
        try {
            Class<?> aClass = Class.forName(typeName);
            return Listener.class.isAssignableFrom(aClass);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * 递归获取类的所有泛型接口
     * Recursively get all generic interfaces of the class
     *
     * @param types 泛型接口集合
     *              Set of generic interfaces
     * @param clazz 类对象
     *              Class object
     * @since 2026-01-08
     */
    private static void genericInterfaces(Set<Type> types, Class<?> clazz) {
        if (Objects.isNull(types) || Objects.isNull(clazz))
            return;
        Type[] genericInterfaces = clazz.getGenericInterfaces();
        if (ArrayUtils.isNotEmpty(genericInterfaces))
            types.addAll(Arrays.asList(genericInterfaces));
        Class<?> superclass = clazz.getSuperclass();
        if (Objects.isNull(superclass))
            return;
        if (superclass.isAssignableFrom(Object.class))
            return;
        if (Listener.class.isAssignableFrom(superclass)) {
            types.add(clazz.getGenericSuperclass());
            genericInterfaces(types, superclass);
        }
    }

}