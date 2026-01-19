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
import com.asialjim.microapplet.hermes.event.EventBus;
import com.asialjim.microapplet.hermes.event.Hermes;
import com.asialjim.microapplet.hermes.provider.HermesRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Hermes 消费者
 * Hermes Consumer
 *
 * @author <a href="mailto:asialjim@hotmail.com">Asial Jim</a>
 * @version 1.0.0
 * @since 1.0.0
 */
public abstract class HermesConsumer {
    /**
     * 调度执行器，用于定时任务和异步操作
     * Scheduled executor service for timed tasks and asynchronous operations
     */
    private final ScheduledExecutorService scheduler;

    /**
     * Hermes服务名称
     * Hermes service name
     */
    @Getter(AccessLevel.PROTECTED)
    private final HermesService hermesService;

    /**
     * Hermes仓库，用于事件存储和查询
     * Hermes repository for event storage and query
     */
    private final HermesRepository hermesRepository;

    /**
     * 构造函数
     * Constructor
     *
     * @param scheduler        调度执行器
     *                         Scheduled executor service
     * @param hermesService    Hermes服务名称
     *                         Hermes service name
     * @param hermesRepository Hermes仓库
     *                         Hermes repository
     * @since 1.0.0
     */
    protected HermesConsumer(
            ScheduledExecutorService scheduler, HermesService hermesService, HermesRepository hermesRepository) {
        this.scheduler = Optional.ofNullable(scheduler).orElseGet(Executors::newSingleThreadScheduledExecutor);
        this.hermesService = hermesService;
        this.hermesRepository = hermesRepository;
    }

    /**
     * 启动消费者
     * Start consumer
     *
     * @since 1.0.0
     */
    @PostConstruct
    public final void start() {
        // 补偿消费
        eventReConsumption();

        // 处理延时事件
        delayedHermesSub();

        // 发送心跳
        pingPong();

        // 监听中间件
        listen2MQ(this::onHermesReceived);
    }


    /**
     * 停止消费者
     * Stop consumer
     *
     * @since 1.0.0
     */
    @PreDestroy
    public final void stop() {
        this.scheduler.shutdown();
        gracefullyShutdownMQListener();
    }

    /**
     * 监听中间件，获取中间件发布的事件编号并交由Hermes 函数处理
     * Listen to middleware, get event ID published by middleware and hand it over to Hermes function for processing
     *
     * @param consumer {@link Consumer consumer}
     *                 Consumer function
     * @since 1.0.0
     */
    protected abstract void listen2MQ(Consumer<String> consumer);

    /**
     * 关闭对中间件的监听
     * Close middleware listener gracefully
     *
     * @since 1.0.0
     */
    protected abstract void gracefullyShutdownMQListener();

    /**
     * 监听器收到事件
     * Listener received event
     *
     * @param id 事件ID
     *           Event ID
     * @since 1.0.0
     */
    private void onHermesReceived(String id) {
        Hermes<?> hermes = this.hermesRepository.queryAvailableHermesByIdAndServiceName(id, this.hermesService.serviceName());
        // 发布本地事件
        Optional.ofNullable(hermes)
                .flatMap(item -> Optional.of(item.setGlobal(false)))
                .ifPresent(EventBus::push);
    }

    /**
     * 事件补偿消费
     * 应用启动时变开始消费一次，随后每隔2分钟消费一次
     * Event compensation consumption
     * Starts consumption once when the application starts, then consumes every 2 minutes
     *
     * @since 1.0.0
     */
    protected void eventReConsumption() {
        this.scheduler.scheduleAtFixedRate(
                () -> this.hermesRepository.reConsumption(this.hermesService.serviceName()),
                0, 2, TimeUnit.MINUTES);
    }


    protected void delayedHermesSub(){
        this.scheduler.scheduleAtFixedRate(
                () -> this.hermesRepository.delayedHermesSub(),
                0, 1, TimeUnit.SECONDS);
    }

    /**
     * 每 30秒发送一次心跳
     *
     * @since 2026/1/8
     */
    protected void pingPong() {
        this.scheduler.scheduleAtFixedRate(
                () -> hermesRepository.pingPong(hermesService),
                0, 30, TimeUnit.SECONDS);
    }
}