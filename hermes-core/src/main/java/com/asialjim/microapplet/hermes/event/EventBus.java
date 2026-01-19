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

package com.asialjim.microapplet.hermes.event;

import com.asialjim.microapplet.hermes.HermesService;
import com.asialjim.microapplet.hermes.listener.JvmOnlyListener;
import com.asialjim.microapplet.hermes.listener.Listener;
import com.asialjim.microapplet.hermes.provider.HermesRepository;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件总线
 * Event Bus
 *
 * @author <a href="mailto:asialjim@qq.com">Asial Jim</a>
 * @version 1.0.0
 * @since 2026-01-08
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class EventBus {
    private static final Map<Type, TreeSet<Listener<?>>> listenerHub = new ConcurrentHashMap<>();
    private static final Set<Listener<?>> globalListeners = new TreeSet<>();
    private static final Set<Listener<?>> hadRegister = new HashSet<>();
    // 各服务都关心哪些事件？
    private static final Map<String, Set<String>> serviceSubTypes = new HashMap<>();

    /**
     * 注册监听器到Hermes注册表
     * Register listeners to Hermes registry
     *
     * @param hermesRepository {@link HermesRepository hermesRepository}
     *                         Hermes repository instance
     * @since 2026-01-08
     */
    public static void register2Hermes(HermesRepository hermesRepository) {
        if (Objects.isNull(hermesRepository)) {
            log.warn("HermesRepository is null, Cannot Register Listener to Hermes");
            return;
        }


        // 按事件类型搜集服务（指定事件类型都有哪些服务感兴趣）
        final Map<Type, Set<String>> serviceMapGroupByType = new HashMap<>();
        listenerHub.values()
                .stream()
                .flatMap(Collection::stream)
                // 本地监听器不应注册到注册表
                .filter(item -> !(item instanceof JvmOnlyListener<?>))
                .peek(item -> {
                    HermesService serviceName = item.getServiceName();
                    Set<Type> types = item.eventType();
                    if (Objects.isNull(serviceName) || Objects.isNull(types))
                        return;

                    String name = serviceName.serviceName();
                    if (StringUtils.isBlank(name))
                        return;

                    Set<String> typeNames = serviceSubTypes.computeIfAbsent(name, s -> new HashSet<>());

                    types.stream()
                            .filter(Objects::nonNull)
                            .map(Type::getTypeName)
                            .forEach(typeNames::add);
                })
                .filter(hadRegister::add)
                .forEach(item ->
                        Optional.ofNullable(item.eventType())
                                .stream()
                                .flatMap(Collection::stream)
                                .map(type -> serviceMapGroupByType.computeIfAbsent(type, key -> new HashSet<>()))
                                .forEach(names -> names.add(item.getServiceName().serviceName()))
                );

        serviceMapGroupByType.forEach((type, serviceNames) -> {
            if (Objects.nonNull(type) && Objects.nonNull(serviceNames) && !serviceNames.isEmpty()) {
                hermesRepository.register(type, serviceNames);
            }
        });

        // 注册完成
        push(new Register2HermesSucceed().setServiceSubTypes(serviceSubTypes));
    }

    /**
     * 发布事件，可指定事件ID和是否推送到全局监听器
     * Publish event, can specify event ID and whether to push to global listeners
     *
     * @param id          事件ID，用于事件溯源
     *                    Event ID, used for event tracing
     * @param push2global 是否推送到全局监听器
     *                    Whether to push to global listeners
     * @param event       事件对象
     *                    Event object
     * @param <E>         事件类型
     *                    Event type
     * @since 2026-01-08
     */
    public static <E> void push(String id, boolean push2global, E event) {
        if (Objects.isNull(event)) return;


        // 向全局监听器推送事件
        if (push2global) {
            for (Listener<?> listener : globalListeners) {
                //noinspection unchecked
                doPush(id, event, (Listener<E>) listener);
            }
        }

        TreeSet<Listener<?>> listeners = listenerHub.get(event.getClass());
        if (Objects.isNull(listeners)) return;

        // 向指定监听器推送事件
        for (Listener<?> listener : listeners) {
            //noinspection unchecked
            doPush(id, event, (Listener<E>) listener);
        }
    }

    /**
     * 发布带事件ID的事件，默认推送到全局监听器
     * Publish event with ID, default push to global listeners
     *
     * @param id    事件ID，用于事件溯源
     *              Event ID, used for event tracing
     * @param event 事件对象
     *              Event object
     * @param <E>   事件类型
     *              Event type
     * @since 2026-01-08
     */
    public static <E> void push(String id, E event) {
        push(id, true, event);
    }

    /**
     * 内部方法，实际执行事件推送给指定监听器
     * Internal method, actually execute event push to specified listener
     *
     * @param id       事件ID，用于事件溯源
     *                 Event ID, used for event tracing
     * @param event    事件对象
     *                 Event object
     * @param listener 监听器实例
     *                 Listener instance
     * @param <E>      事件类型
     *                 Event type
     * @since 2026-01-08
     */
    private static <E> void doPush(String id, E event, Listener<E> listener) {
        if (StringUtils.isBlank(id))
            listener.onEvent(event);
        else
            listener.onEvent(id, event);
    }

    private static <E> void doPushAfter(String id, E event, Listener<E> listener, Duration after){
        if (StringUtils.isBlank(id))
            listener.onEventAfter(event,after);
        else
            listener.onEventAfter(id, event,after);
    }

    /**
     * 发布事件，用户直接调用此静态方法即可发布事件
     * Publish event, users can directly call this static method to publish events
     *
     * @param event {@link E event}
     *              Event object
     * @param <E>   事件类型
     *              Event type
     * @since 2026-01-08
     */
    public static <E> void push(E event) {
        push(null, event);
    }


    /**
     * 注册监听器到事件总线
     * Register listener to event bus
     *
     * @param listener 监听器实例
     *                 Listener instance
     * @since 2026-01-08
     */
    public static void register(Listener<?> listener) {
        if (Objects.isNull(listener)) return;
        boolean globalListener = listener.globalListener();
        if (globalListener) globalListeners.add(listener);

        Set<Type> types = listener.eventType();
        for (Type type : types) {
            if (Objects.isNull(type)) return;

            if (!StringUtils.startsWith(type.toString(), "class")) return;

            TreeSet<Listener<?>> listeners = listenerHub.get(type);
            if (Objects.isNull(listeners)) {
                synchronized (listenerHub) {
                    //noinspection ConstantValue
                    if (Objects.isNull(listeners)) {
                        listeners = new TreeSet<>(Comparator.comparingInt(Listener::getOrder));
                        listenerHub.put(type, listeners);
                    }
                }
            }
            if (log.isDebugEnabled())
                log.info("事件总线注册事件：{} 监听器：{}", type, listener);
            listeners.add(listener);
        }
    }

}