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

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * 事件包装器，用于包装和传递事件的元数据和实际内容
 * Event wrapper used to wrap and pass event metadata and actual content
 *
 * @param <E> 事件数据的类型
 *            The type of event data
 * @author <a href="mailto:asialjim@qq.com">Asial Jim</a>
 * @version 1.0.0
 * @since 2026-01-08
 */
@Data
@Accessors(chain = true)
public final class Hermes<E> implements Serializable {
    @Serial
    private static final long serialVersionUID = 6509239159364530352L;
    
    /**
     * 默认事件主题名称
     * Default event topic name
     */
    public static final String TOPIC = "_hermes_event_";

    /**
     * 集群是否存活，仅在运行时有效，不参与序列化
     * Whether the cluster is alive, only valid at runtime, not involved in serialization
     */
    private transient boolean clusterAlive = false;

    /**
     * 事件唯一编号，用于事件溯源和去重
     * Unique event ID, used for event tracing and deduplication
     */
    private String id;

    /**
     * 事件所属会话ID，用于关联同一会话的多个事件
     * Event session ID, used to associate multiple events in the same session
     */
    private String session;

    /**
     * 事件链路追踪ID，用于分布式追踪
     * Event trace ID, used for distributed tracing
     */
    private String trace;

    /**
     * 事件发送者名称，标识事件是由哪个服务发送的
     * Event sender name, identifying which service sent the event
     */
    private String sendFrom;

    /**
     * 是否为全局事件
     * Whether it is a global event
     * <ul>
     *     <li>true: 需要发送到消息总线，所有订阅该事件类型的服务都会收到</li>
     *     <li>false: 仅需调用本地监听器，只有当前服务内的监听器会收到</li>
     *     <li>true: Need to send to message bus, all services subscribing to this event type will receive it</li>
     *     <li>false: Only need to call local listeners, only listeners in the current service will receive it</li>
     * </ul>
     */
    private Boolean global;
    
    /**
     * 获取事件是否为全局事件
     * Get whether the event is a global event
     *
     * @return 如果是全局事件返回true，否则返回false，默认值为false
     *         Returns true if it is a global event, otherwise returns false, default value is false
     * @since 2026-01-08
     */
    public boolean global(){
        return Optional.ofNullable(this.global).orElse(Boolean.FALSE);
    }

    /**
     * 事件发送时间
     * Event sending time
     */
    private LocalDateTime sendTime;

    /**
     * 事件类型名称，通常是事件类的全限定名
     * Event type name, usually the fully qualified name of the event class
     */
    private String type;

    /**
     * 事件需要发送到的服务名称集合，指定哪些服务需要接收该事件
     * Set of service names that the event needs to be sent to, specifying which services need to receive the event
     */
    private Set<String> sendTo;
    
    /**
     * 获取事件需要发送到的服务名称集合
     * Get the set of service names that the event needs to be sent to
     *
     * @return 服务名称集合，如果为null则返回空集合
     *         Set of service names, returns an empty set if null
     * @since 2026-01-08
     */
    public Set<String> getSendTo(){
        return Optional.ofNullable(sendTo).orElseGet(HashSet::new);
    }

    /**
     * 事件的实际内容数据
     * Actual content data of the event
     */
    private E data;

    /**
     * 事件当前状态，用于跟踪事件处理进度
     * Current status of the event, used to track event processing progress
     */
    private String status;
    private Duration trigAfter;
}