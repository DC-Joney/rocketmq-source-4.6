/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    // 延迟隔离容错的具体实现
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    // 默认是不开启延迟隔离（延迟失败）
    private boolean sendLatencyFaultEnable = false;

    //花费时间的列表，从50毫秒到15000毫秒
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};

    //失效时间列表，从0到600000毫秒(600s=60s * 10)，
    //举个例子，如果发送到某个Broker用了3000毫秒，那么该broker就会被加入到延迟隔离的列表中，失效时间为180000毫秒，即，只有180000毫秒后，这个Broker才可用
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    //选择队列
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //如果开启了延迟隔离 ,默认是没有的
        //高可用场景建议开启
        if (this.sendLatencyFaultEnable) {
            try {
                //  round-robin: 这个index，每次优择一个队列，tpInfo中的ThreadLocalIndex都会加1
                //  意味着，每个线程去获取队列，其实都是负载均衡的。
                //  注意，通过线程局部变量，进行了无锁编程， 避免了锁的操作
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    //与队列的长度取模，根据最后的pos取一个队列
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //判断取到的队列的broker是否隔离中，
                     if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        // 如果不是隔离中就返回即可
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                // 如果所有的队列都是隔离中的话
                // 那么就从 faultItemTable 隔离列表取出一个Broker即可
                // 作为  次优的 broker
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                // 获取这个broker的可写队列数，如果该Broker没有可写的队列，则返回-1
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    // 再次优择一次队列
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
//                        // 次优的 broker
                        mq.setBrokerName(notBestBroker);
                          // 通过与队列的长度取模确定队列的位置
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //没有可写的队列，直接从隔离列表移除  Broker
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            //兜底
            //如果故障列表中也没有可写的队列，则直接从tpInfo中获取一个
            return tpInfo.selectOneMessageQueue();
        }

        // 没有开启延迟隔离，
        // 直接从TopicPublishInfo通过取模的方式获取队列即可
        // 如果LastBrokerName不为空，则需要过滤掉brokerName=lastBrokerName的队列
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    //更新故障项

    //本质：计算隔离截止时间， startTimestamp

    //currentLatency :rt 时长
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        //开启延迟隔离容错
        if (this.sendLatencyFaultEnable) {
            //计算出故障时间，隔离默认是30秒，不隔离则根据性能来选择故障时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    public long computeNotAvailableDuration(final long currentLatency) {

        // 根据性能来选择相应的 隔离时长(不可用时长)，
        // rt 时间 意味着currentLatency越大，那么对应的 notAvailableDuration (不可用时长)也就越大。
        // 因为某种意义上来说，肯定是需要规避性能差的broker的使用次数。

        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
