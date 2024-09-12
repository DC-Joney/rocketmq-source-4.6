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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class ProducerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    /**
     * producer client 上报心跳的超时时间
     */
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

    // 获取可用长连接的重试次数，默认 3 次
    private static final int GET_AVALIABLE_CHANNEL_RETRY_COUNT = 3;
    private final Lock groupChannelLock = new ReentrantLock();

    /**
     * 核心数据结构：生产组 -> 生产者客户端连接的映射关系，key 为生产组名，value 为网络长连接与生产者客户端连接的映射关系
     */
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
        new HashMap<String, HashMap<Channel, ClientChannelInfo>>();

    // 客户端 id 与长连接的映射关系，key 为客户端ID，value 为网络长连接
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();

    // 正数计数器，用于生成客户端ID
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    public ProducerManager() {
    }

    /**
     * 扫描不活跃的客户端长连接
     */
    public void scanNotActiveChannel() {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {

                    //遍历所有的producer client
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();


                            // 计算长连接的过期时间
                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            //如果producer client心跳超时则移除
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                it.remove();
                                //将其从clientChannelTable中移除
                                clientChannelTable.remove(info.getClientId());
                                log.warn(
                                        "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                        RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 获取生产者所有的client channel
     */
    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    Iterator<Map.Entry<String, HashMap<Channel, ClientChannelInfo>>> iter = groupChannelTable.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry = iter.next();
                        String key = entry.getKey();
                        HashMap<Channel, ClientChannelInfo> val = entry.getValue();
                        HashMap<Channel, ClientChannelInfo> tmp = new HashMap<Channel, ClientChannelInfo>();
                        tmp.putAll(val);
                        newGroupChannelTable.put(key, tmp);
                    }
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return newGroupChannelTable;
    }


    /**
     * 处理producer client的连接断开事件
     *
     * @param remoteAddr client的地址信息
     * @param channel channel
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {

                        //遍历所有producer 的连接
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                            final String group = entry.getKey();

                            // clientChannelInfoTable 缓存表
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                                entry.getValue();

                            // 从 clientChannelInfoTable 移除对应 channel 的数据
                            final ClientChannelInfo clientChannelInfo =
                                clientChannelInfoTable.remove(channel);
                            if (clientChannelInfo != null) {
                                // 从客户端ID与长连接的映射关系中移除
                                clientChannelTable.remove(clientChannelInfo.getClientId());
                                log.info(
                                    "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                    clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    } finally {
                        this.groupChannelLock.unlock();
                    }
                } else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    /**
     * 当客户端从NameSrv获取到topic的元数据信息后，同时会获取到对应的broker地址，producer会定时向broker发送心跳<p/>
     * 在发送心跳时就会将对应的channel注册到当前的producerManager中
     *
     * 注册 producer channel
     * @param group 生产者group
     * @param clientChannelInfo producer client 的channel信息
     */
    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            ClientChannelInfo clientChannelInfoFound = null;

            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    //如果查不到对应的group信息，则创建一个新的添加进去
                    if (null == channelTable) {
                        channelTable = new HashMap<>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    //如果获取不到channel的信息
                    if (null == clientChannelInfoFound) {
                        //将其添加到channelTable中
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        //并且将其添加 client id 与 channel的映射关系中
                        clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
                        log.info("new producer connected, group: {} channel: {}", group,
                            clientChannelInfo.toString());
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }

                //更新当前channel的最后的心跳时间
                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 注销生产者长连接信息
     * @param group
     * @param clientChannelInfo
     */
    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        clientChannelTable.remove(clientChannelInfo.getClientId());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                clientChannelInfo.toString());
                        }

                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    /**
     * 根据producer group获取可用的channel
     * @param groupId producer group
     * @return
     */
    public Channel getAvaliableChannel(String groupId) {
        // 通过生产者组获取 channelTable
        HashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        //producer group 可用的channel
        List<Channel> channelList = new ArrayList<Channel>();
        if (channelClientChannelInfoHashMap != null) {
            for (Channel channel : channelClientChannelInfoHashMap.keySet()) {
                channelList.add(channel);
            }
            int size = channelList.size();
            if (0 == size) {
                log.warn("Channel list is empty. groupId={}", groupId);
                return null;
            }

            //通过轮训算法获取可以可用的channel
            int index = positiveAtomicCounter.incrementAndGet() % size;
            Channel channel = channelList.get(index);
            int count = 0;
            boolean isOk = channel.isActive() && channel.isWritable();
            // 检查当前Channel是否Active并且可写，若是，则直接返回
            // 否则继续寻找下一个，一共查找三次
            while (count++ < GET_AVALIABLE_CHANNEL_RETRY_COUNT) {
                if (isOk) {
                    return channel;
                }
                index = (++index) % size;
                channel = channelList.get(index);
                isOk = channel.isActive() && channel.isWritable();
            }
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }
        return null;
    }

    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }
}
