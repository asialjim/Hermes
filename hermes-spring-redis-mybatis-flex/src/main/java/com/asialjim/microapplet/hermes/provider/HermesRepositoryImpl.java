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

package com.asialjim.microapplet.hermes.provider;

import com.asialjim.microapplet.hermes.HermesService;
import com.asialjim.microapplet.hermes.event.EventBus;
import com.asialjim.microapplet.hermes.event.Hermes;
import com.asialjim.microapplet.hermes.infrastructure.repository.po.ConsumptionCount;
import com.asialjim.microapplet.hermes.infrastructure.repository.po.EventPO;
import com.asialjim.microapplet.hermes.infrastructure.repository.service.ConsumptionMapperService;
import com.asialjim.microapplet.hermes.infrastructure.repository.service.EventMapperService;
import com.asialjim.microapplet.hermes.infrastructure.repository.service.SubscriberMapperService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Hermes事件仓库实现类
 * Hermes Event Repository Implementation Class
 * <p>
 * 基于Redis和MyBatis Flex实现的HermesRepository接口实现
 * 负责事件的存储、查询、发布、注册等核心功能
 * <p>
 * 核心功能包括：
 * 1. 事件发布与存储
 * 2. 事件订阅与注册
 * 3. 事件消费与状态管理
 * 4. 事件补偿消费
 * 5. 事件状态跟踪
 * <p>
 * Implementation of HermesRepository interface based on Redis and MyBatis Flex
 * Responsible for core functions such as event storage, query, publishing, and registration
 * <p>
 * Core functions include:
 * 1. Event publishing and storage
 * 2. Event subscription and registration
 * 3. Event consumption and status management
 * 4. Event compensation consumption
 * 5. Event status tracking
 *
 * @author <a href="mailto:asialjim@qq.com">Asial Jim</a>
 * @version 1.0.0
 * @since 2026-01-08
 */
@Slf4j
@Component
public class HermesRepositoryImpl implements HermesRepository {
    private static final ZoneOffset zoneOffset = ZoneOffset.of(ZoneOffset.systemDefault().getId());

    /**
     * 订阅者服务
     */
    @Resource
    private SubscriberMapperService subscriberMapperService;

    /**
     * 消费记录服务
     */
    @Resource
    private ConsumptionMapperService consumptionMapperService;

    /**
     * 事件服务
     */
    @Resource
    private EventMapperService eventMapperService;

    /**
     * Redis模板，用于发布事件通知
     */
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private HermesService hermesService;

    // hermes 心跳保持 lua 脚本
    private static final String luaScript = """
                local hash_key = KEYS[1]
                local instance_id = ARGV[1]
                local expire_at = ARGV[2]
                local current_time = ARGV[3]
                -- 更新当前实例的存活时间
                redis.call('HSET', hash_key, instance_id, expire_at)
                -- 获取所有实例的存活记录
                local entries = redis.call('HGETALL', hash_key)
                local expired_instances = {}
                local expired_count = 0
                -- 遍历所有实例，找出过期的实例
                for i = 1, #entries, 2 do
                    local key = entries[i]
                    local value = tonumber(entries[i+1])
                    if value < current_time then
                        expired_count = expired_count + 1
                        expired_instances[expired_count] = key
                    end
                end
                -- 删除过期的实例
                if expired_count > 0 then
                    redis.call('HDEL', hash_key, unpack(expired_instances))
                end
            
                -- 返回过期的实例列表
                return expired_instances
            """;
    
    // 添加延迟事件到队列的 Lua 脚本
    // Add delayed event to queue Lua script
    private static final String send2DelayedQueueLuaScript = """
                -- 获取参数
                local delayedTypesKey = KEYS[1]
                local delayedQueueKey = KEYS[2]
                local eventType = ARGV[1]
                local eventId = ARGV[2]
                local executeTime = tonumber(ARGV[3])
                
                -- 向延迟事件类型集合添加事件类型
                redis.call('SADD', delayedTypesKey, eventType)
                
                -- 向延迟队列添加事件ID，分数为执行时间戳
                redis.call('ZADD', delayedQueueKey, executeTime, eventId)
                
                return 1
            """;

    /**
     * 标记事件正在被处理
     * Mark event as being processed
     * <p>
     * 同时更新消费记录和事件记录的状态为处理中
     * <p>
     * Update the status of both consumption record and event record to processing
     *
     * @param eventId     事件ID
     *                    Event ID
     * @param application 应用服务名称
     *                    Application service name
     * @since 2026-01-08
     */
    @Override
    @Transactional
    public void processingEvent(String eventId, String application) {
        this.consumptionMapperService.processingEvent(eventId, application);
        this.eventMapperService.processingEvent(eventId, application);
    }

    /**
     * 记录事件处理失败
     * Record event processing failure
     * <p>
     * 更新消费记录的状态为失败，并记录错误信息
     * <p>
     * Update the status of consumption record to failed and record error information
     *
     * @param eventId     事件ID
     *                    Event ID
     * @param application 应用服务名称
     *                    Application service name
     * @param err         错误信息
     *                    Error information
     * @since 2026-01-08
     */
    @Override
    public void errorEvent(String eventId, String application, String err) {
        this.consumptionMapperService.errorEvent(eventId, application, err);
    }

    /**
     * 记录事件处理成功
     * Record event processing success
     * <p>
     * 更新消费记录的状态为成功
     * <p>
     * Update the status of consumption record to successful
     *
     * @param eventId     事件ID
     *                    Event ID
     * @param application 应用服务名称
     *                    Application service name
     * @since 2026-01-08
     */
    @Override
    public void succeedEvent(String eventId, String application) {
        ConsumptionCount consumptionCount = this.consumptionMapperService.succeedEvent(eventId, application);
        this.eventMapperService.succeedEvent(eventId, consumptionCount);
    }

    @Override
    public void pingPong(HermesService hermesService) {
        // 当前服务名
        String name = hermesService.serviceName();
        // 当前服务实例编号
        String instanceId = hermesService.instanceId();

        // 当前时间
        long now = System.currentTimeMillis();
        // 当前实例要存活到的时间
        long expireAt = now + TimeUnit.MINUTES.toMillis(2);

        // 为每一个服务在redis创建一个hash, key 为实例编号， value 为该实例要存活到多久
        String allInstance = "tmp:hermes:service:ping-pong:" + name;

        // 使用 Lua 脚本优化 Redis IO 操作，减少网络往返次数

        // 执行 Lua 脚本
        List<String> keys = Collections.singletonList(allInstance);
        List<String> args = Arrays.asList(instanceId, String.valueOf(expireAt), String.valueOf(now));

        List<?> expiredInstanceSet = stringRedisTemplate.execute(
                new DefaultRedisScript<>(luaScript, List.class),
                keys,
                args.toArray()
        );

        //noinspection ConstantValue
        if (Objects.isNull(expiredInstanceSet))
            return;

        if (expiredInstanceSet.isEmpty())
            return;

        List<String> list = expiredInstanceSet.stream()
                .filter(Objects::nonNull)
                .filter(item -> item instanceof String)
                .map(String::valueOf)
                .toList();

        this.subscriberMapperService.unRegisterInstance(list);
    }

    /**
     * 填充事件需要发送到的服务列表
     * Populate the list of services that the event needs to be sent to
     * <p>
     * 根据事件类型查询订阅该事件的服务列表
     * <p>
     * Query the list of services that subscribe to this event type
     *
     * @param hermes 事件对象
     *               Event object
     * @since 2026-01-08
     */
    @Override
    public void populateSendTo(Hermes<?> hermes) {
        String type = hermes.getType();
        Set<String> subServiceNames = this.subscriberMapperService.applicationsByEventType(type);
        hermes.setSendTo(subServiceNames);
    }

    /**
     * 注册服务对事件类型的订阅关系
     * Register service subscription to event type
     * <p>
     * 将服务名称与事件类型的订阅关系保存到数据库
     * <p>
     * Save the subscription relationship between service names and event types to the database
     *
     * @param type         事件类型
     *                     Event type
     * @param serviceNames 服务名称集合
     *                     Service name collection
     * @since 2026-01-08
     */
    @Override
    public void register(Type type, Set<String> serviceNames) {
        if (Objects.isNull(type) || CollectionUtils.isEmpty(serviceNames))
            return;
        String typeName = type.getTypeName();
        // 注册，将实例编号也同步注册
        String instanceId = this.hermesService.instanceId();
        this.subscriberMapperService.register(instanceId, typeName, serviceNames);
    }

    /**
     * 为指定服务弹出一个待处理的事件
     * Pop a pending event for the specified service
     * <p>
     * 从数据库中获取一个该服务待处理的事件，并更新其状态为处理中
     * <p>
     * Get a pending event for this service from the database and update its status to processing
     *
     * @param serviceName 服务名称
     *                    Service name
     * @return 事件对象，若没有待处理事件则返回null
     * Event object, returns null if there are no pending events
     * @since 2026-01-08
     */
    @Override
    @Transactional
    public Hermes<?> pop(String serviceName) {
        String eventId = this.consumptionMapperService.pop(serviceName);
        if (StringUtils.isBlank(eventId))
            return null;
        if (log.isDebugEnabled())
            log.info("补偿消费事件编号：{}", eventId);
        EventPO hermesPO = this.eventMapperService.queryById(eventId);
        if (Objects.isNull(hermesPO) || StringUtils.equals("-", hermesPO.getData()))
            return null;
        Hermes<?> hermes = EventPO.to(hermesPO);
        if (log.isDebugEnabled())
            log.info("Pop {} result: {}", serviceName, hermes);
        this.consumptionMapperService.popped(eventId, serviceName);
        return hermes;
    }

    /**
     * 根据事件ID和服务名称查询可用的事件
     * Query available event by event ID and service name
     * <p>
     * 检查事件是否存在且未被同服务名的其他实例获取
     * <p>
     * Check if the event exists and has not been obtained by other instances with the same service name
     *
     * @param id          事件ID
     *                    Event ID
     * @param serviceName 服务名称
     *                    Service name
     * @return 可用的事件对象，若不可用则返回null
     * Available event object, returns null if unavailable
     * @since 2026-01-08
     */
    @Override
    public Hermes<?> queryAvailableHermesByIdAndServiceName(String id, String serviceName) {
        boolean available = this.consumptionMapperService.eventIdAndServiceNameAvailable(id, serviceName);
        if (!available)
            return null;
        EventPO hermesPO = this.eventMapperService.queryById(id);
        if (Objects.isNull(hermesPO) || StringUtils.equals("-", hermesPO.getData()))
            return null;
        Hermes<?> hermes = EventPO.to(hermesPO);
        if (log.isDebugEnabled())
            log.info("Available Hermes of {} for {} result: {}", id, serviceName, hermes);
        return hermes;
    }

    /**
     * 记录事件处理结果
     * Record event processing result
     * <p>
     * 更新消费记录的状态、结果码和描述信息
     * <p>
     * Update the status, result code and description information of consumption record
     *
     * @param id          事件ID
     *                    Event ID
     * @param serviceName 服务名称
     *                    Service name
     * @param code        结果码
     *                    Result code
     * @param err         结果描述
     *                    Result description
     * @since 2026-01-08
     */
    @Override
    @Transactional
    public void log(String id, String serviceName, String code, String err) {
        this.consumptionMapperService.log(id, serviceName, code, err);
    }

    /**
     * 执行事件补偿消费
     * Execute event compensation consumption
     * <p>
     * 循环获取并处理该服务的待处理事件，直到没有待处理事件为止
     * <p>
     * Loop to get and process pending events for this service until there are no pending events
     *
     * @param serviceName 服务名称
     *                    Service name
     * @since 2026-01-08
     */
    @Override
    public void reConsumption(String serviceName) {
        if (log.isDebugEnabled())
            log.info("服务 {} 补偿消费Hermes......", serviceName);
        HermesRepositoryImpl hermesRepository = (HermesRepositoryImpl) AopContext.currentProxy();
        Hermes<?> hermes;
        do {
            if (log.isDebugEnabled())
                log.info("补偿消费...");
            hermes = hermesRepository.doReConsumption(serviceName);
        } while (Objects.nonNull(hermes));
        log.info("服务 {} 补偿消费Hermes 结束!!!!!!", serviceName);
    }

    /**
     * 执行单次补偿消费
     * Execute single compensation consumption
     * <p>
     * 从数据库中获取一个待处理事件，若存在则发布到事件总线
     * <p>
     * Get a pending event from the database and publish it to the event bus if it exists
     *
     * @param serviceName 服务名称
     *                    Service name
     * @return 处理的事件对象，若没有待处理事件则返回null
     * Processed event object, returns null if there are no pending events
     * @since 2026-01-08
     */
    @Transactional
    public Hermes<?> doReConsumption(String serviceName) {
        Hermes<?> hermes = pop(serviceName);
        if (log.isDebugEnabled())
            log.info("获取到补偿消费事件：{}", hermes);
        if (Objects.nonNull(hermes)) {
            EventBus.push(hermes.setGlobal(false));
        }
        return hermes;
    }

    /**
     * 事件发送前的预处理
     * Preprocessing before event sending
     * <p>
     * 将事件保存到数据库，并生成事件ID
     * <p>
     * Save the event to the database and generate an event ID
     *
     * @param hermes 事件对象
     *               Event object
     * @since 2026-01-08
     */
    @Override
    public void beforeSend(Hermes<?> hermes) {
        EventPO po = EventPO.from(hermes);
        this.eventMapperService.saveCacheable(po);
        hermes.setId(po.getId());
    }

    /**
     * 执行事件发送
     * Execute event sending
     * <p>
     * 为事件创建消费记录，准备发送给订阅该事件的服务
     * <p>
     * Create consumption records for the event, ready to send to services that subscribe to this event
     *
     * @param hermes 事件对象
     *               Event object
     * @since 2026-01-08
     */
    @Override
    public void doSend(Hermes<?> hermes) {
        Set<String> sendTo = hermes.getSendTo();
        Duration trigAfter = hermes.getTrigAfter();
        LocalDateTime sendTime = hermes.getSendTime();
        LocalDateTime trigTime = sendTime;
        if (Objects.nonNull(trigAfter)) {
            trigTime = sendTime.plusNanos(trigAfter.getNano());
        }
        this.consumptionMapperService.send(hermes.getId(), sendTo, trigTime);
    }

    /**
     * 发布事件通知
     * Publish event notification
     * <p>
     * 通过Redis发布事件ID，通知订阅该事件的服务
     * <p>
     * Publish the event ID through Redis to notify services that subscribe to this event
     *
     * @param hermes 事件对象
     *               Event object
     * @since 2026-01-08
     */
    @Override
    public void publish(Hermes<?> hermes) {
        if (log.isDebugEnabled())
            log.info("Publish Hermes: {}", hermes);
        // 空事件或者 不是全局事件
        if (Objects.isNull(hermes) || !hermes.global())
            return;

        Duration trigAfter = hermes.getTrigAfter();
        // 非延时事件
        if (Objects.isNull(trigAfter) || trigAfter.isZero() || trigAfter.isNegative()) {
            // 直接发布到redis
            push2redis(hermes.getId(), hermes.getType());
            return;
        }

        // 发布到延时队列
        send2DelayedQueue(hermes, trigAfter);
    }

    private void push2redis(Collection<String> ids, String type) {
        if (!CollectionUtils.isEmpty(ids))
            ids.forEach(id -> push2redis(id, type));
    }

    private void push2redis(String id, String type) {
        // 针对性发布事件
        final String topic = "hermes:id:" + type;
        final byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        final byte[] bodyBytes = id.getBytes(StandardCharsets.UTF_8);

        final RedisCallback<Long> callback = link -> link.publish(topicBytes, bodyBytes);

        final Long res = stringRedisTemplate.execute(callback);

        if (log.isDebugEnabled())
            log.info("Hermes Publish Result: {}", res);
    }

    @Override
    public void delayedHermesSub() {
        // 获取延迟事件类型集合的键
        final String delayedHermesTypes = "hermes:delayed:types";
        // 当前时间戳（秒）
        long now = Instant.now().getEpochSecond();
        // 批量大小，限制单次处理的事件数量
        int batchSize = 100;
        
        // 使用单个Lua脚本处理所有延迟事件业务逻辑
        // Use a single Lua script to handle all delayed event business logic
        List<String> keys = Collections.singletonList(delayedHermesTypes);
        List<String> args = Arrays.asList(
            String.valueOf(now), 
            String.valueOf(batchSize),
            "hermes:id:"
        );
        
        // 执行Lua脚本，获取处理结果
        // Execute the Lua script and get the processing results
        List<Object> results = stringRedisTemplate.execute(
                new DefaultRedisScript<>(completeDelayedEventProcessScript, List.class),
                keys,
                args.toArray()
        );
        
        // 处理返回结果
        if (Objects.isNull(results) || results.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("延迟事件处理完成: 无结果返回");
            }
            return;
        }
        
        // 解析返回结果
        try {
            // 获取已发布的事件信息
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> publishedEvents = (List<Map<String, Object>>) results.get(0);
            
            // 获取空的事件类型
            @SuppressWarnings("unchecked")
            List<String> emptyTypes = (List<String>) results.get(1);
            
            // 获取总发布数量
            Long totalPublished = (Long) results.get(2);
            
            // 记录处理结果
            if (log.isDebugEnabled()) {
                log.debug("延迟事件处理完成: 发布事件数量={}, 空事件类型数量={}", 
                    totalPublished, emptyTypes != null ? emptyTypes.size() : 0);
                
                // 记录空的事件类型
                if (!CollectionUtils.isEmpty(emptyTypes)) {
                    log.debug("删除空的延迟事件类型: {}", emptyTypes);
                }
                
                // 记录已发布的事件详情（仅记录前5个，避免日志过多）
                if (!CollectionUtils.isEmpty(publishedEvents)) {
                    int logLimit = Math.min(5, publishedEvents.size());
                    for (int i = 0; i < logLimit; i++) {
                        Map<String, Object> event = publishedEvents.get(i);
                        log.debug("发布事件: 类型={}, ID={}, 发布结果={}", 
                            event.get("type"), event.get("id"), event.get("publishResult"));
                    }
                    if (publishedEvents.size() > logLimit) {
                        log.debug("... 还有 {} 个事件已发布", publishedEvents.size() - logLimit);
                    }
                }
            }
        } catch (Exception e) {
            log.error("解析延迟事件处理结果时发生错误", e);
        }
    }


    private void send2DelayedQueue(Hermes<?> hermes, Duration trigAfter) {
        LocalDateTime executeTime = hermes.getSendTime().plusNanos(trigAfter.getNano());

        // 处理延时事件
        final String delayedHermesTypes = "hermes:delayed:types";
        final String delayedHermesRedisKey = "hermes:delayed:" + hermes.getType();
        // 计算执行时间的时间戳秒值
        long epochSecond = executeTime.toInstant(zoneOffset).getEpochSecond();
        
        // 使用 Lua 脚本原子性地执行两个操作：
        // 1. 向延迟事件类型集合添加事件类型
        // 2. 向延迟队列添加事件ID，分数为执行时间戳
        // Use Lua script to atomically execute two operations:
        // 1. Add event type to delayed event type set
        // 2. Add event ID to delayed queue with score as execution timestamp
        List<String> keys = Arrays.asList(delayedHermesTypes, delayedHermesRedisKey);
        List<String> args = Arrays.asList(hermes.getType(), hermes.getId(), String.valueOf(epochSecond));
        
        stringRedisTemplate.execute(
                new DefaultRedisScript<>(send2DelayedQueueLuaScript, Long.class),
                keys,
                args.toArray()
        );
    }

}