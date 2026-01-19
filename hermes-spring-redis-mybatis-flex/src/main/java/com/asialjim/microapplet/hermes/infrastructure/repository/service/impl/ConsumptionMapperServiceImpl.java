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

package com.asialjim.microapplet.hermes.infrastructure.repository.service.impl;

import com.asialjim.microapplet.hermes.ConsumptionStatus;
import com.asialjim.microapplet.hermes.infrastructure.repository.mapper.ConsumptionBaseMapper;
import com.asialjim.microapplet.hermes.infrastructure.repository.po.ConsumptionCount;
import com.asialjim.microapplet.hermes.infrastructure.repository.po.ConsumptionPO;
import com.asialjim.microapplet.hermes.infrastructure.repository.service.ConsumptionMapperService;
import com.mybatisflex.spring.service.impl.ServiceImpl;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 消费记录服务实现类
 * <p>
 * 该类继承自 MyBatis Flex 的 ServiceImpl，实现了 ConsumptionMapperService 接口，
 * 提供消费记录实体的服务层操作实现，包括事件的获取、处理状态更新、日志记录等功能。
 * Consumption mapper service implementation
 * <p>
 * This class extends MyBatis Flex's ServiceImpl, implements the ConsumptionMapperService interface,
 * providing service layer operation implementations for consumption record entities,
 * including event acquisition, processing status updates, log recording, and other functions.
 *
 * @author Asial Jim
 * @version 1.0.0
 * @since 1.0.0
 */
@Slf4j
@Repository
public class ConsumptionMapperServiceImpl
        extends ServiceImpl<ConsumptionBaseMapper, ConsumptionPO>
        implements ConsumptionMapperService {

    /**
     * Redis 字符串模板，用于 Redis 操作
     * Redis string template for Redis operations
     */
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 从队列中获取一个事件ID供指定服务消费
     * <p>
     * 该方法从数据库中查询并返回一个指定服务可用的事件ID，使用乐观锁防止并发问题。
     * Get an event ID from the queue for consumption by the specified service
     * <p>
     * This method queries and returns an available event ID for the specified service from the database,
     * using optimistic locking to prevent concurrency issues.
     *
     * @param serviceName 服务名称
     * @return 事件ID，如果没有可用事件则返回null
     * @since 1.0.0
     */
    @Override
    public String pop(String serviceName) {
        //noinspection unchecked
        return queryChain()
                .forUpdateNoWait()
                .select(ConsumptionPO::getEventId)
                .where(ConsumptionPO::getSubscriber).eq(serviceName)
                .where(ConsumptionPO::getStatus).le(ConsumptionStatus.PENDING.getId())
                .where(ConsumptionPO::getNextTime).lt(LocalDateTime.now())
                .oneAs(String.class);
    }

    /**
     * 标记事件已被获取
     * <p>
     * 该方法将指定事件标记为已被指定服务获取，防止重复消费。
     * Mark event as popped
     * <p>
     * This method marks the specified event as having been retrieved by the specified service,
     * preventing duplicate consumption.
     *
     * @param eventId     事件ID
     * @param serviceName 服务名称
     * @since 1.0.0
     */
    @Override
    public void popped(String eventId, String serviceName) {
        boolean update = updateChain()
                .set(ConsumptionPO::getStatus, ConsumptionStatus.PROCESSING)
                .where(ConsumptionPO::getSubscriber).eq(serviceName)
                .where(ConsumptionPO::getEventId).eq(eventId)
                .update();
        if (log.isDebugEnabled())
            log.info("Hermes: {} for Service: {}  had updated: {}", eventId, serviceName, update);
    }

    /**
     * 检查事件ID和服务名称是否可用
     * <p>
     * 该方法检查指定事件ID和服务名称的组合是否可用，用于判断事件是否可以被消费。
     * 同时使用Redis实现分布式锁，防止并发问题。
     * Check if event ID and service name are available
     * <p>
     * This method checks if the combination of the specified event ID and service name is available,
     * used to determine if an event can be consumed. It also uses Redis to implement distributed locking
     * to prevent concurrency issues.
     *
     * @param id          事件ID
     * @param serviceName 服务名称
     * @return 如果可用则返回true，否则返回false
     * @since 1.0.0
     */
    @Override
    public boolean eventIdAndServiceNameAvailable(String id, String serviceName) {
        String key = "tmp:hermes:" + id + ":for:" + serviceName + ":available";
        String s = stringRedisTemplate.opsForValue().get(key);
        if (StringUtils.isNotBlank(s))
            return false;

        boolean available = queryChain()
                .where(ConsumptionPO::getEventId).eq(id)
                .where(ConsumptionPO::getSubscriber).eq(serviceName)
                .where(ConsumptionPO::getStatus).eq(ConsumptionStatus.PENDING.getId())
                .exists();
        if (available)
            stringRedisTemplate.opsForValue().set(key, "lk", 30, TimeUnit.MINUTES);
        return available;
    }

    /**
     * 标记事件正在处理中
     * <p>
     * 该方法将指定事件标记为正在被指定应用处理。
     * Mark event as being processed
     * <p>
     * This method marks the specified event as being processed by the specified application.
     *
     * @param eventId     事件ID
     * @param application 应用名称
     * @since 1.0.0
     */
    @Override
    public void processingEvent(String eventId, String application) {
        boolean update = this.updateChain()
                .set(ConsumptionPO::getStatus, ConsumptionStatus.PROCESSING)
                .where(ConsumptionPO::getEventId).eq(eventId)
                .where(ConsumptionPO::getSubscriber).eq(application)
                .update();
        if (log.isDebugEnabled())
            log.info("服务： {} 对 Hermes: {} 处理中: {}",
                    eventId, application, update);

    }

    /**
     * 标记事件处理失败
     * <p>
     * 该方法将指定事件标记为在指定应用中处理失败，并记录错误信息。
     * Mark event processing as failed
     * <p>
     * This method marks the specified event as having failed processing in the specified application,
     * and records the error information.
     *
     * @param eventId     事件ID
     * @param application 应用名称
     * @param err         错误信息
     * @since 1.0.0
     */
    @Override
    public void errorEvent(String eventId, String application, String err) {
        boolean update = updateChain()
                .set(ConsumptionPO::getStatus, ConsumptionStatus.FAILED)
                .set(ConsumptionPO::getCode, "500")
                .set(ConsumptionPO::getDescription, err)
                .where(ConsumptionPO::getSubscriber).eq(application)
                .where(ConsumptionPO::getEventId).eq(eventId)
                .update();
        if (log.isDebugEnabled())
            log.info("Hermes: {} for Service: {}  had error: {}, update result: {}", eventId, application, err, update);
    }

    /**
     * 标记事件处理成功
     * <p>
     * 该方法将指定事件标记为在指定应用中处理成功。
     * Mark event processing as successful
     * <p>
     * This method marks the specified event as having been successfully processed in the specified application.
     *
     * @param eventId     事件ID
     * @param application 应用名称
     * @since 1.0.0
     */
    @Override
    public ConsumptionCount succeedEvent(String eventId, String application) {
        boolean update = updateChain()
                .set(ConsumptionPO::getStatus, ConsumptionStatus.SUCCEEDED)
                .set(ConsumptionPO::getCode, "0")
                .set(ConsumptionPO::getDescription, "SUCCESS")
                .where(ConsumptionPO::getSubscriber).eq(application)
                .where(ConsumptionPO::getEventId).eq(eventId)
                .update();
        if (log.isDebugEnabled())
            log.info("Hermes: {} for Service: {}  had succeed, update result: {}", eventId, application, update);

        ConsumptionStatus[] values = ConsumptionStatus.values();
        int len = values.length + 1;
        String[] columns = new String[len];
        columns[0] = "COUNT(1) AS total";
        for (int i = 0; i < values.length; i++) {
            ConsumptionStatus value = values[i];
            int index = i + 1;
            columns[index] = "SUM(CASE WHEN status = '" + value.getId() + "' THEN 1 ELSE 0 END) AS " + value.getCode();
        }

        return queryChain()
                .select(columns)
                .where(ConsumptionPO::getEventId).eq(eventId)
                .oneAs(ConsumptionCount.class);
    }

    /**
     * 记录消费日志
     * <p>
     * 该方法记录事件消费的日志信息，包括事件ID、服务名称、状态码和错误信息，
     * 并根据状态码更新事件的消费状态。
     * Log consumption information
     * <p>
     * This method records log information for event consumption, including event ID, service name, status code, and error information,
     * and updates the event's consumption status based on the status code.
     *
     * @param id          事件ID
     * @param serviceName 服务名称
     * @param code        状态码
     * @param err         错误信息
     * @since 1.0.0
     */
    @Override
    public void log(String id, String serviceName, String code, String err) {
        ConsumptionStatus status = StringUtils.equals(code, "0") ? ConsumptionStatus.SUCCEEDED : ConsumptionStatus.FAILED;
        boolean update = this.updateChain()
                .set(ConsumptionPO::getStatus, status)
                .set(ConsumptionPO::getCode, code)
                .set(ConsumptionPO::getDescription, err)
                .where(ConsumptionPO::getEventId).eq(id)
                .where(ConsumptionPO::getSubscriber).eq(serviceName)
                .update();
        if (log.isDebugEnabled())
            log.info("服务： {} 对 Hermes: {} 处理结果：{} 记录结果： {}\r\n",
                    serviceName, id, err, update);
    }

    /**
     * 发送事件给指定的服务列表
     * <p>
     * 该方法为指定的事件创建多个消费记录，每个服务对应一条记录，状态初始化为待处理。
     * Send event to specified service list
     * <p>
     * This method creates multiple consumption records for the specified event,
     * one record for each service, with the status initialized to pending.
     *
     * @param id     事件ID
     * @param sendTo 服务名称集合
     * @since 1.0.0
     */
    @Override
    public void send(String id, Set<String> sendTo, LocalDateTime trigTime) {
        List<ConsumptionPO> collect = sendTo.stream()
                .map(item -> new ConsumptionPO()
                        .setEventId(id)
                        .setSubscriber(item)
                        .setStatus(ConsumptionStatus.PENDING)
                        .setNextTime(trigTime)
                )
                .toList();

        // 批量插入
        partitionListByStream(collect, 100).forEach(this::saveBatch);
    }

    private static <T> List<List<T>> partitionListByStream(List<T> originalList, @SuppressWarnings("SameParameterValue") int batchSize) {
        final int totalSize = originalList.size();
        return IntStream.range(0, (totalSize + batchSize - 1) / batchSize)
                .mapToObj(i -> originalList.subList(
                        i * batchSize,
                        Math.min(totalSize, (i + 1) * batchSize)
                ))
                .collect(Collectors.toList());
    }
}