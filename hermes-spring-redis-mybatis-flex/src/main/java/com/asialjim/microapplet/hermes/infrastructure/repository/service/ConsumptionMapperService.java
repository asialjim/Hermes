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

package com.asialjim.microapplet.hermes.infrastructure.repository.service;

import com.asialjim.microapplet.hermes.infrastructure.repository.po.ConsumptionCount;
import com.asialjim.microapplet.hermes.infrastructure.repository.po.ConsumptionPO;
import com.mybatisflex.core.service.IService;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * 消费记录服务接口
 * <p>
 * 该接口继承自 MyBatis Flex 的 IService，提供消费记录实体的服务层操作，
 * 包括事件的获取、处理状态更新、日志记录等功能。
 * Consumption mapper service interface
 * <p>
 * This interface extends MyBatis Flex's IService, providing service layer operations for consumption record entities,
 * including event acquisition, processing status updates, log recording, and other functions.
 * 
 * @author Asial Jim
 * @version 1.0.0
 * @since 1.0.0
 */
public interface ConsumptionMapperService
    extends IService<ConsumptionPO> {

    /**
     * 从队列中获取一个事件ID供指定服务消费
     * <p>
     * 该方法从队列中获取一个事件ID，供指定的服务进行消费处理。
     * Get an event ID from the queue for consumption by the specified service
     * <p>
     * This method retrieves an event ID from the queue for consumption processing by the specified service.
     * 
     * @param serviceName 服务名称
     * @return 事件ID，如果没有可用事件则返回null
     * @version 1.0.0
     * @since 1.0.0
     */
    String pop(String serviceName);

    /**
     * 标记事件已被获取
     * <p>
     * 该方法标记指定事件已被指定服务获取，防止重复消费。
     * Mark event as popped
     * <p>
     * This method marks the specified event as having been retrieved by the specified service, preventing duplicate consumption.
     * 
     * @param eventId 事件ID
     * @param serviceName 服务名称
     * @version 1.0.0
     * @since 1.0.0
     */
    void popped(String eventId, String serviceName);

    /**
     * 检查事件ID和服务名称是否可用
     * <p>
     * 该方法检查指定事件ID和服务名称的组合是否可用，用于判断事件是否可以被消费。
     * Check if event ID and service name are available
     * <p>
     * This method checks if the combination of the specified event ID and service name is available, 
     * used to determine if an event can be consumed.
     * 
     * @param id 事件ID
     * @param serviceName 服务名称
     * @return 如果可用则返回true，否则返回false
     * @version 1.0.0
     * @since 1.0.0
     */
    boolean eventIdAndServiceNameAvailable(String id, String serviceName);

    /**
     * 记录消费日志
     * <p>
     * 该方法记录事件消费的日志信息，包括事件ID、服务名称、状态码和错误信息。
     * Log consumption information
     * <p>
     * This method records log information for event consumption, including event ID, service name, status code, and error information.
     * 
     * @param id 事件ID
     * @param serviceName 服务名称
     * @param code 状态码
     * @param err 错误信息
     * @since 1.0.0
     */
    void log(String id, String serviceName, String code, String err);

    /**
     * 发送事件给指定的服务列表
     * <p>
     * 该方法将指定事件发送给指定的服务列表。
     * Send event to specified service list
     * <p>
     * This method sends the specified event to the specified list of services.
     * 
     * @param id 事件ID
     * @param sendTo 服务名称集合
     * @since 1.0.0
     */
    void send(String id, Set<String> sendTo,  LocalDateTime trigTime);

    /**
     * 标记事件正在处理中
     * <p>
     * 该方法标记指定事件正在被指定应用处理。
     * Mark event as being processed
     * <p>
     * This method marks the specified event as being processed by the specified application.
     * 
     * @param eventId 事件ID
     * @param application 应用名称
     * @version 1.0.0
     * @since 1.0.0
     */
    void processingEvent(String eventId, String application);

    /**
     * 标记事件处理失败
     * <p>
     * 该方法标记指定事件在指定应用中处理失败，并记录错误信息。
     * Mark event processing as failed
     * <p>
     * This method marks the specified event as having failed processing in the specified application, and records the error information.
     * 
     * @param eventId 事件ID
     * @param application 应用名称
     * @param err 错误信息
     * @version 1.0.0
     * @since 1.0.0
     */
    void errorEvent(String eventId, String application, String err);

    /**
     * 标记事件处理成功
     * <p>
     * 该方法标记指定事件在指定应用中处理成功。
     * Mark event processing as successful
     * <p>
     * This method marks the specified event as having been successfully processed in the specified application.
     * 
     * @param eventId 事件ID
     * @param application 应用名称
     * @since 1.0.0
     */
    ConsumptionCount succeedEvent(String eventId, String application);


}