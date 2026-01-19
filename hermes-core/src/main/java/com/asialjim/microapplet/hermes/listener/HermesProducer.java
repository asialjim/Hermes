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

package com.asialjim.microapplet.hermes.listener;

import com.asialjim.microapplet.hermes.HermesService;
import com.asialjim.microapplet.hermes.event.Hermes;
import com.asialjim.microapplet.hermes.provider.HermesCluster;
import com.asialjim.microapplet.hermes.provider.HermesRepository;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Hermes 事件发布器
 * Hermes Event Producer
 * <pre>
 *     监听JVM本地事件，然后发布到分布式总线
 *     Listen to JVM local events, then publish to distributed bus
 * </pre>
 *
 * @author <a href="mailto:asialjim@hotmail.com">Asial Jim</a>
 * @version 1.0.0
 * @since 1.0.0
 */
@Slf4j
@Setter
public class HermesProducer implements JvmOnlyListener<Object> {
    /**
     * 服务名称
     * Service name
     */
    @Getter
    private final HermesService serviceName;

    /**
     * Hermes仓库，用于事件存储和发送
     * Hermes repository for event storage and sending
     */
    private final HermesRepository hermesRepository;

    /**
     * 会话标识供应商
     * Session ID supplier
     */
    private final Supplier<String> sessionSupplier;

    /**
     * 链路标识供应商
     * Trace ID supplier
     */
    private final Supplier<String> traceSupplier;

    /**
     * Hermes集群，用于事件中继
     * Hermes cluster for event relay
     */
    private HermesCluster hermesCluster;

    /**
     * 构建 Hermes 生产者
     * Build Hermes producer
     *
     * @param serviceName      生产者服务名称
     *                         Producer service name
     * @param hermesRepository Hermes仓库
     *                         Hermes repository
     * @param sessionSupplier  会话标识供应商
     *                         Session ID supplier
     * @param traceSupplier    链路标识供应商
     *                         Trace ID supplier
     * @param cluster          事件中继集群
     *                         Event relay cluster
     * @since 1.0.0
     */
    public HermesProducer(@Nonnull HermesService serviceName,
                          @Nonnull HermesRepository hermesRepository,
                          @Nonnull Supplier<String> sessionSupplier,
                          @Nonnull Supplier<String> traceSupplier,
                          @Nullable HermesCluster cluster) {
        this.serviceName = serviceName;
        this.hermesRepository = hermesRepository;
        this.sessionSupplier = sessionSupplier;
        this.traceSupplier = traceSupplier;
        this.hermesCluster = cluster;
    }

    /**
     * 获取感兴趣的事件类型
     * Get interested event types
     *
     * @return 事件类型集合，包含Object类
     * Set of event types, contains Object class
     * @since 1.0.0
     */
    @Override
    public Set<Type> eventType() {
        return Collections.singleton(Object.class);
    }

    /**
     * 是否是全局监听器
     * Whether it is a global listener
     *
     * @return 总是返回true，表示这是一个全局监听器
     * Always returns true, indicating this is a global listener
     * @since 1.0.0
     */
    @Override
    public final boolean globalListener() {
        return true;
    }

    /**
     * 执行事件处理的核心方法
     * Execute core event processing method
     *
     * @param hermes Hermes事件对象
     *               Hermes event object
     * @since 1.0.0
     */
    @Override
    public void doOnEvent(Hermes<Object> hermes) {
        if (Objects.nonNull(hermes.getGlobal())) {
            //  非全局事件，不发布到全局总线
            if (!hermes.global()) {
                return;
            }
        }
        Set<String> sendTo = hermes.getSendTo();
        // 没人感兴趣此事件，不再发送到hermes
        if (Objects.isNull(sendTo) || sendTo.isEmpty())
            return;

        // 包装为全局事件，发布到 Hermes
        boolean clusterAlive = hermes.isClusterAlive();
        // 通过事件中继集群发送
        if (clusterAlive) {
            log.info("事件中继集群发布事件：{}", hermes);
            this.hermesCluster.send(hermes);
        }

        // 通过本地消息表发送
        else {
            log.info("本地消息表发送事件：{}", hermes);
            this.hermesRepository.send(hermes);
        }
    }

    /**
     * 事件处理前的回调方法
     * Callback method before event processing
     *
     * @param wrapper Hermes事件包装对象
     *                Hermes event wrapper object
     * @since 1.0.0
     */
    @Override
    public void before(Hermes<Object> wrapper) {
        if (Objects.nonNull(wrapper.getGlobal())) {
            //  非全局事件，不发布到全局总线
            if (!wrapper.global()) {
                return;
            }
        }
        wrapper.setSession(this.sessionSupplier.get());
        wrapper.setTrace(this.traceSupplier.get());
        wrapper.setSendFrom(this.serviceName.serviceName());
        if (Objects.isNull(wrapper.getGlobal()))
            wrapper.setGlobal(true);
        this.hermesRepository.populateSendTo(wrapper);
        wrapper.setStatus("PENDING");

        boolean alive = Optional.ofNullable(this.hermesCluster)
                .map(HermesCluster::alive)
                .orElse(false);

        wrapper.setClusterAlive(alive);
    }
}