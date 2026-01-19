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

package com.asialjim.microapplet.hermes.infrastructure.repository.po;

import com.asialjim.microapplet.hermes.HermesStatus;
import com.asialjim.microapplet.hermes.event.Hermes;
import com.asialjim.microapplet.hermes.infrastructure.config.table.HermesTable;
import com.asialjim.microapplet.hermes.infrastructure.repository.handler.HermesStatusHandler;
import com.asialjim.util.jackson.Json;
import com.mybatisflex.annotation.Column;
import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.KeyType;
import com.mybatisflex.annotation.Table;
import com.mybatisflex.core.keygen.KeyGenerators;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 事件实体类
 * <p>
 * 该类用于映射数据库中的事件表，存储事件的基本信息和状态。
 * Event persistent object
 * <p>
 * This class is used to map the event table in the database, storing basic information and status of events.
 * 
 * @author Asial Jim
 * @version 1.0.0
 * @since 1.0.0
 */
@Data
@Accessors(chain = true)
@Table(HermesTable.event)
public class EventPO implements Serializable {

    @Serial
    private static final long serialVersionUID = -8837734211266680510L;
    
    /**
     * 事件编号，自动生成UUID
     * Event ID, automatically generated UUID
     */
    @Id(keyType = KeyType.Generator, value = KeyGenerators.uuid)
    private String id;

    /**
     * 事件类型
     * Event type
     */
    private String type;

    /**
     * 事件内容
     * Event data
     */
    private String data;

    /**
     * 事件状态
     * Event status
     */
    @Column(typeHandler = HermesStatusHandler.class)
    private HermesStatus status;

    /**
     * 事件发送者
     * Event sender
     */
    private String sendBy;
    
    /**
     * 事件接收者，逗号分隔的服务名称列表
     * Event recipients, comma-separated list of service names
     */
    private String sendTo;

    /**
     * 关注此事件的服务数量
     * Number of services subscribing to this event
     */
    private Integer subServiceNum;

    /**
     * 成功处理此事件的服务数量
     * Number of services that successfully processed this event
     */
    private Integer succeedServiceNum;

    /**
     * 失败处理此事件的服务数量
     * Number of services that failed to process this event
     */
    private Integer failedServiceNum;

    /**
     * 事件创建时间，插入时自动生成
     * Event create time, automatically generated when inserting
     */
    @Column(onInsertValue = "now()")
    private LocalDateTime createTime;

    /**
     * 事件更新时间，插入和更新时自动生成
     * Event update time, automatically generated when inserting and updating
     */
    @Column(onInsertValue = "now()", onUpdateValue = "now()")
    private LocalDateTime updateTime;

    private LocalDateTime trigAfter;

    /**
     * 将 EventPO 转换为 Hermes 对象
     * <p>
     * 该方法将数据库中的事件记录转换为内存中的 Hermes 事件对象。
     * Convert EventPO to Hermes object
     * <p>
     * This method converts an event record in the database to an in-memory Hermes event object.
     * 
     * @param hermes EventPO 对象
     * @return Hermes 对象
     * @since 1.0.0
     */
    public static Hermes<?> to(EventPO hermes) {
        Hermes<Object> po = new Hermes<>();
        po.setId(hermes.getId());
        po.setSendFrom(hermes.getSendBy());
        po.setSendTime(hermes.getCreateTime());
        po.setType(hermes.getType());

        if (StringUtils.isNotBlank(hermes.getSendTo())) {
            Set<String> collect = Arrays.stream(hermes.getSendTo().split(",")).collect(Collectors.toSet());
            po.setSendTo(collect);
        }

        try {
            String json = hermes.getData();
            Object o = Json.instance.toBean(json, Class.forName(hermes.getType()));
            po.setData(o);
        } catch (Throwable ignored) {
        }

        String code = hermes.getStatus().getCode();
        po.setStatus(code);
        LocalDateTime trigAfter = hermes.getTrigAfter();
        if (Objects.nonNull(trigAfter)){
            LocalDateTime now = LocalDateTime.now();
            Duration duration = Duration.between(now,trigAfter);
            // 触发时间在当前时间之后
            if (!duration.isZero() && duration.isPositive()){
                po.setTrigAfter(duration);
            }
            // 已经到达触发时间
            else {
                po.setTrigAfter(null);
            }
        }
        return po;
    }

    /**
     * 将 Hermes 对象转换为 EventPO
     * <p>
     * 该方法将内存中的 Hermes 事件对象转换为数据库中的事件记录。
     * Convert Hermes object to EventPO
     * <p>
     * This method converts an in-memory Hermes event object to an event record in the database.
     * 
     * @param hermes Hermes 对象
     * @return EventPO 对象
     * @since 1.0.0
     */
    public static EventPO from(Hermes<?> hermes) {
        EventPO po = new EventPO();
        po.setId(hermes.getId());
        po.setType(hermes.getType());
        po.setData(Json.instance.toStr(hermes.getData()));
        po.setStatus(HermesStatus.codeOf(hermes.getStatus()));
        po.setSendBy(hermes.getSendFrom());
        po.setSendTo(String.join(",",hermes.getSendTo()));
        po.setSubServiceNum(hermes.getSendTo().size());
        po.setSucceedServiceNum(0);
        po.setFailedServiceNum(0);
        po.setCreateTime(hermes.getSendTime());

        Duration trigAfter = hermes.getTrigAfter();
        LocalDateTime sendTime = hermes.getSendTime();
        if (Objects.nonNull(trigAfter) && !trigAfter.isZero()) {
            LocalDateTime trigTime = sendTime.plusNanos(trigAfter.getNano());
            po.setTrigAfter(trigTime);
        }
        return po;
    }
}