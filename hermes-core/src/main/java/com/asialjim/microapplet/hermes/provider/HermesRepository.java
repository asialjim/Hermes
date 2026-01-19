/*
 *    Copyright 2014-2025 <a href="mailto:asialjim@qq.com">Asial Jim</a>
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

package com.asialjim.microapplet.hermes.provider;

import com.asialjim.microapplet.hermes.HermesService;
import com.asialjim.microapplet.hermes.event.Hermes;
import com.asialjim.microapplet.hermes.sender.HermesSender;

import java.lang.reflect.Type;
import java.util.Set;

/**
 * 本地事件本地消息表仓库
 * Local Event Local Message Table Repository
 *
 * @author <a href="mailto:asialjim@hotmail.com">Asial Jim</a>
 * @version 1.0.0
 * @since 1.0.0
 */
public interface HermesRepository extends HermesSender {

    /**
     * 填充当前事件应当发送给哪些服务
     * Populate which services the current event should be sent to
     *
     * @param hermes {@link Hermes hermes}
     *              Hermes event object
     * @since 1.0.0
     */
    void populateSendTo(Hermes<?> hermes);

    /**
     * 向注册表注册指定服务对哪些事件感兴趣
     * Register which events the specified services are interested in to the registry
     *
     * @param type         {@link Type type}
     *                    Event type
     * @param serviceNames {@link Set<String> serviceNames}
     *                    Set of service names
     * @since 1.0.0
     */
    void register(Type type, Set<String> serviceNames);

    /**
     * 为指定服务名弹出一个该服务感兴趣的 Hermes 事件
     * Pop a Hermes event that the service is interested in for the specified service name
     *
     * @param serviceName {@link String serviceName}
     *                   Service name
     * @return {@link Hermes 事件 }
     *         Hermes event
     * @since 1.0.0
     */
    Hermes<?> pop(String serviceName);

    /**
     * 根据事件编号和服务名查询可用的事件
     * Query available event by event ID and service name
     * <pre>
     *     注意： 如果该事件没有发给指定的服务名则应当返回空
     *           如果该事件已经被同服务名的其他实例获取到，也应当返回为空，避免重复消费
     *     Note: If the event is not sent to the specified service name, it should return null
     *           If the event has already been acquired by other instances with the same service name, it should also return null to avoid duplicate consumption
     * </pre>
     *
     * @param id          {@link String id}
     *                   Event ID
     * @param serviceName {@link String serviceName}
     *                   Service name
     * @return {@link Hermes }
     *         Hermes event
     * @since 1.0.0
     */
    Hermes<?> queryAvailableHermesByIdAndServiceName(String id, String serviceName);

    /**
     * 记录服务对指定事件的处理结果
     * Record the processing result of the specified event by the service
     *
     * @param id          {@link String id}
     *                   Event ID
     * @param serviceName {@link String serviceName}
     *                   Service name
     * @param code        {@link String code}
     *                   Processing result code
     * @param err         {@link String err}
     *                   Error message if processing failed
     * @since 1.0.0
     */
    void log(String id, String serviceName, String code, String err);

    /**
     * 服务补偿消费感兴趣的事件
     * Service compensation consumption of interested events
     *
     * @param serviceName {@link String serviceName}
     *                   Service name
     * @since 1.0.0
     */
    void reConsumption(String serviceName);

    /**
     * 标记事件正在被处理
     * Mark event as being processed
     *
     * @param eventId    事件ID
     *                  Event ID
     * @param application 应用服务名称
     *                   Application service name
     * @since 1.0.0
     */
    void processingEvent(String eventId, String application);

    /**
     * 记录事件处理失败
     * Record event processing failure
     *
     * @param eventId    事件ID
     *                  Event ID
     * @param application 应用服务名称
     *                   Application service name
     * @param err        错误信息
     *                   Error message
     * @since 1.0.0
     */
    void errorEvent(String eventId, String application, String err);

    /**
     * 记录事件处理成功
     * Record event processing success
     *
     * @param eventId    事件ID
     *                  Event ID
     * @param application 应用服务名称
     *                   Application service name
     * @since 1.0.0
     */
    void succeedEvent(String eventId, String application);

    void pingPong(HermesService hermesService);

    /**
     * 处理延时事件
     * @since 2026/1/19
     */
    void delayedHermesSub();
}