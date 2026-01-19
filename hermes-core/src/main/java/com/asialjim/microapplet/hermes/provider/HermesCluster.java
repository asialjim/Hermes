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

import com.asialjim.microapplet.hermes.event.Hermes;
import com.asialjim.microapplet.hermes.sender.HermesSender;

/**
 * Hermes 事件中继集群
 * Hermes Event Relay Cluster
 *
 * @author <a href="mailto:asialjim@hotmail.com">Asial Jim</a>
 * @version 1.0.0
 * @since 1.0.0
 */
public interface HermesCluster extends HermesSender {
    /**
     * 集群是否可用
     * Whether the cluster is available
     *
     * @return 如果集群可用返回true，否则返回false
     *         Returns true if the cluster is available, otherwise returns false
     * @since 1.0.0
     * @version 1.0.0
     */
    boolean alive();

    /**
     * 发送事件到中继集群
     * Send event to relay cluster
     *
     * @param hermes {@link Hermes hermes}
     *              Hermes event object
     * @since 1.0.0
     */
    @Override
    void send(Hermes<?> hermes);
}