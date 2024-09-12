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

package org.apache.rocketmq.broker.filter;

import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Consumer filter data manager.Just manage the consumers use expression filter.
 * 消费者过滤数据管理组件，只管理使用表达式过滤的消费者。
 */

public class ConsumerFilterManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    private static final long MS_24_HOUR = 24 * 3600 * 1000;

    // 核心数据结构：topic -> consumer group -> ConsumerFilterData，存放以 Topic 为key 的消费者组过滤信息
    private ConcurrentMap<String/*Topic*/, FilterDataMapByTopic/*consumerGroup -> ConsumerFilterData*/>
        filterDataByTopic = new ConcurrentHashMap<String/*consumer group*/, FilterDataMapByTopic>(256);

    private transient BrokerController brokerController;

    // 布隆过滤器
    private transient BloomFilter bloomFilter;

    public ConsumerFilterManager() {
        // 初始化布隆过滤器
        // just for test
        this.bloomFilter = BloomFilter.createByFn(20, 64);
    }

    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.bloomFilter = BloomFilter.createByFn(
            brokerController.getBrokerConfig().getMaxErrorRateOfBloomFilter(),
            brokerController.getBrokerConfig().getExpectConsumerNumUseFilter()
        );
        // then set bit map length of store config.
        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(
            this.bloomFilter.getM()
        );
    }

    /**
     * Build consumer filter data.Be care, bloom filter data is not included.
     * （小心： RocketMQ引入了布隆过滤器来加快效率，但是默认没有包含）
     *
     // 使用Tag过滤，效率最高: why? 通过ConsumeQueue的hashcode就可快速判断，如果不满足要求，可以不用再读取CommitLog文件。
     // 使用SQL92语法过滤，需要对消息属性进行操作，而消息属性是存储在CommitLog文件，因此，即使不满足要求，也需要读取CommitLog文件才能进行判断，效率较低。

     * @return maybe null
     */
    ///**
    //* 构建消费者过滤器数据。注意，布隆过滤器数据不包含在内。
    //* Build consumer filter data.Be care, bloom filter data is not included.
    //* @return 可能为空
    //*/
    public static ConsumerFilterData build(final String topic, final String consumerGroup,
        final String expression, final String type,
        final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            // 如果表达式类型是标签类型，返回null
            return null;
        }

        // 创建消费者过滤器数据对象
        ConsumerFilterData consumerFilterData = new ConsumerFilterData();

        //设置主题
        consumerFilterData.setTopic(topic);

        // 设置消费者组
        consumerFilterData.setConsumerGroup(consumerGroup);

        //设置产生时间为当前时间
        consumerFilterData.setBornTime(System.currentTimeMillis());

        // 设置过期时间为0，表示不过期
        consumerFilterData.setDeadTime(0);

        //设置过滤表达式
        consumerFilterData.setExpression(expression);

        //设置表达式类型
        consumerFilterData.setExpressionType(type);

        //设置客户端版本号
        consumerFilterData.setClientVersion(clientVersion);
        try {

            // 编译表达式
            consumerFilterData.setCompiledExpression(
                FilterFactory.INSTANCE.get(type).compile(expression)
            );
        } catch (Throwable e) {
            log.error("parse error: expr={}, topic={}, group={}, error={}", expression, topic, consumerGroup, e.getMessage());
            return null;
        }

        return consumerFilterData;
    }

    // 该方法用于注册消费者组的订阅信息，并处理非法的主题。
    // 当有新的consumer注册到consumerManager中时，将consumer订阅的topic以及对应的表达式存放到ConsumerFilterManager中
    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {

        // 遍历订阅数据列表，逐个注册
        for (SubscriptionData subscriptionData : subList) {
            register(
                subscriptionData.getTopic(),
                consumerGroup,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion()
            );
        }

        // 处理非法的主题
        // make illegal topic dead.
        Collection<ConsumerFilterData> groupFilterData = getByGroup(consumerGroup);

        // 获取消费者组的过滤器数据列表
        Iterator<ConsumerFilterData> iterator = groupFilterData.iterator();

        // 遍历消费者组的过滤器数据列表
        while (iterator.hasNext()) {
            ConsumerFilterData filterData = iterator.next();

            boolean exist = false;

            // 检查订阅数据列表中是否包含过滤器数据中的主题
            for (SubscriptionData subscriptionData : subList) {
                if (subscriptionData.getTopic().equals(filterData.getTopic())) {
                    exist = true;
                    break;
                }
            }

            // 如果过滤器数据对应的主题不包含在订阅数据列表中，并且未过期，则标记为过期
            if (!exist && !filterData.isDead()) {
                filterData.setDeadTime(System.currentTimeMillis());
                log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
            }
        }
    }

    /**
     * 注册指定topic的消费者组过滤器数据
     * @param topic topic
     * @param consumerGroup consumer group
     * @param expression 过滤表达式
     * @param type 过滤器类型
     */
    public boolean register(final String topic, final String consumerGroup, final String expression,
        final String type, final long clientVersion) {

        // 不支持tag类型
        // 如果表达式类型是标签类型，则返回 false
        if (ExpressionType.isTagType(type)) {
            return false;
        }

        if (expression == null || expression.length() == 0) {
            return false;
        }

        // 获取指定主题的 FilterDataMapByTopic 对象
        FilterDataMapByTopic filterDataMapByTopic = this.filterDataByTopic.get(topic);

        if (filterDataMapByTopic == null) {
            // 如果 FilterDataMapByTopic 对象不存在，创建一个新的并放入 filterDataByTopic 中
            FilterDataMapByTopic temp = new FilterDataMapByTopic(topic);
            FilterDataMapByTopic prev = this.filterDataByTopic.putIfAbsent(topic, temp);
            filterDataMapByTopic = prev != null ? prev : temp;
        }

        // 生成布隆过滤器数据
        BloomFilterData bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);

        // 调用 FilterDataMapByTopic#register 方法注册过滤器数
        return filterDataMapByTopic.register(consumerGroup, expression, type, bloomFilterData, clientVersion);
    }

    public void unRegister(final String consumerGroup) {
        for (String topic : filterDataByTopic.keySet()) {
            this.filterDataByTopic.get(topic).unRegister(consumerGroup);
        }
    }

    public ConsumerFilterData get(final String topic, final String consumerGroup) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().get(consumerGroup);
    }

    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<ConsumerFilterData>();

        Iterator<FilterDataMapByTopic> topicIterator = this.filterDataByTopic.values().iterator();
        while (topicIterator.hasNext()) {
            FilterDataMapByTopic filterDataMapByTopic = topicIterator.next();

            Iterator<ConsumerFilterData> filterDataIterator = filterDataMapByTopic.getGroupFilterData().values().iterator();

            while (filterDataIterator.hasNext()) {
                ConsumerFilterData filterData = filterDataIterator.next();

                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }

        return ret;
    }

    public final Collection<ConsumerFilterData> get(final String topic) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().values();
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        if (this.brokerController != null) {
            return BrokerPathConfigHelper.getConsumerFilterPath(
                this.brokerController.getMessageStoreConfig().getStorePathRootDir()
            );
        }
        return BrokerPathConfigHelper.getConsumerFilterPath("./unit_test");
    }

    @Override
    public void decode(final String jsonString) {
        ConsumerFilterManager load = RemotingSerializable.fromJson(jsonString, ConsumerFilterManager.class);
        if (load != null && load.filterDataByTopic != null) {
            boolean bloomChanged = false;
            for (String topic : load.filterDataByTopic.keySet()) {
                FilterDataMapByTopic dataMapByTopic = load.filterDataByTopic.get(topic);
                if (dataMapByTopic == null) {
                    continue;
                }

                for (String group : dataMapByTopic.getGroupFilterData().keySet()) {

                    ConsumerFilterData filterData = dataMapByTopic.getGroupFilterData().get(group);

                    if (filterData == null) {
                        continue;
                    }

                    try {
                        filterData.setCompiledExpression(
                            FilterFactory.INSTANCE.get(filterData.getExpressionType()).compile(filterData.getExpression())
                        );
                    } catch (Exception e) {
                        log.error("load filter data error, " + filterData, e);
                    }

                    // check whether bloom filter is changed
                    // if changed, ignore the bit map calculated before.
                    if (!this.bloomFilter.isValid(filterData.getBloomFilterData())) {
                        bloomChanged = true;
                        log.info("Bloom filter is changed!So ignore all filter data persisted! {}, {}", this.bloomFilter, filterData.getBloomFilterData());
                        break;
                    }

                    log.info("load exist consumer filter data: {}", filterData);

                    if (filterData.getDeadTime() == 0) {
                        // we think all consumers are dead when load
                        long deadTime = System.currentTimeMillis() - 30 * 1000;
                        filterData.setDeadTime(
                            deadTime <= filterData.getBornTime() ? filterData.getBornTime() : deadTime
                        );
                    }
                }
            }

            if (!bloomChanged) {
                this.filterDataByTopic = load.filterDataByTopic;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        // clean
        {
            clean();
        }
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public void clean() {
        Iterator<Map.Entry<String, FilterDataMapByTopic>> topicIterator = this.filterDataByTopic.entrySet().iterator();
        while (topicIterator.hasNext()) {
            Map.Entry<String, FilterDataMapByTopic> filterDataMapByTopic = topicIterator.next();

            Iterator<Map.Entry<String, ConsumerFilterData>> filterDataIterator
                = filterDataMapByTopic.getValue().getGroupFilterData().entrySet().iterator();

            while (filterDataIterator.hasNext()) {
                Map.Entry<String, ConsumerFilterData> filterDataByGroup = filterDataIterator.next();

                ConsumerFilterData filterData = filterDataByGroup.getValue();
                if (filterData.howLongAfterDeath() >= (this.brokerController == null ? MS_24_HOUR : this.brokerController.getBrokerConfig().getFilterDataCleanTimeSpan())) {
                    log.info("Remove filter consumer {}, died too long!", filterDataByGroup.getValue());
                    filterDataIterator.remove();
                }
            }

            if (filterDataMapByTopic.getValue().getGroupFilterData().isEmpty()) {
                log.info("Topic has no consumer, remove it! {}", filterDataMapByTopic.getKey());
                topicIterator.remove();
            }
        }
    }

    public ConcurrentMap<String, FilterDataMapByTopic> getFilterDataByTopic() {
        return filterDataByTopic;
    }

    public void setFilterDataByTopic(final ConcurrentHashMap<String, FilterDataMapByTopic> filterDataByTopic) {
        this.filterDataByTopic = filterDataByTopic;
    }

    public static class FilterDataMapByTopic {

        // 核心数据结构：consumer group -> ConsumerFilterData 即消费组 -> 消费组过滤数据
        private ConcurrentMap<String/*consumer group*/, ConsumerFilterData>
            groupFilterData = new ConcurrentHashMap<String, ConsumerFilterData>();

        //topic名称
        private String topic;

        public FilterDataMapByTopic() {
        }

        public FilterDataMapByTopic(String topic) {
            this.topic = topic;
        }

        public void unRegister(String consumerGroup) {
            if (!this.groupFilterData.containsKey(consumerGroup)) {
                return;
            }

            ConsumerFilterData data = this.groupFilterData.get(consumerGroup);

            if (data == null || data.isDead()) {
                return;
            }

            long now = System.currentTimeMillis();

            log.info("Unregister consumer filter: {}, deadTime: {}", data, now);

            data.setDeadTime(now);
        }

        public boolean register(String consumerGroup, String expression, String type, BloomFilterData bloomFilterData,
            long clientVersion) {

            // 获取消费者组的过滤器数据
            ConsumerFilterData old = this.groupFilterData.get(consumerGroup);

            // 如果旧的过滤器数据不存在
            if (old == null) {
                // 创建新的消费者过滤器数据对象
                ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                if (consumerFilterData == null) {
                    return false;
                }
                // 设置布隆过滤器数据
                consumerFilterData.setBloomFilterData(bloomFilterData);

                // 将新的过滤器数据放入groupFilterData中
                old = this.groupFilterData.putIfAbsent(consumerGroup, consumerFilterData);

                // 如果putIfAbsent返回的old为null，表示成功注册新的过滤器数据
                if (old == null) {
                    log.info("New consumer filter registered: {}", consumerFilterData);
                    return true;
                } else {
                    // 否则表示存在并发注册
                    if (clientVersion <= old.getClientVersion()) {
                        if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                            log.warn("Ignore consumer({} : {}) filter(concurrent), because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                consumerGroup, topic,
                                clientVersion, old.getClientVersion(),
                                old.getExpressionType(), old.getExpression(),
                                type, expression);
                        }
                        if (clientVersion == old.getClientVersion() && old.isDead()) {
                            reAlive(old);
                            return true;
                        }

                        return false;
                    } else {
                        this.groupFilterData.put(consumerGroup, consumerFilterData);
                        log.info("New consumer filter registered(concurrent): {}, old: {}", consumerFilterData, old);
                        return true;
                    }
                }
            } else {
                if (clientVersion <= old.getClientVersion()) {
                    if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                        log.info("Ignore consumer({}:{}) filter, because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                            consumerGroup, topic,
                            clientVersion, old.getClientVersion(),
                            old.getExpressionType(), old.getExpression(),
                            type, expression);
                    }
                    if (clientVersion == old.getClientVersion() && old.isDead()) {
                        reAlive(old);
                        return true;
                    }

                    return false;
                }

                // 判断是否需要更新过滤器数据
                boolean change = !old.getExpression().equals(expression) || !old.getExpressionType().equals(type);
                if (old.getBloomFilterData() == null && bloomFilterData != null) {
                    change = true;
                }
                if (old.getBloomFilterData() != null && !old.getBloomFilterData().equals(bloomFilterData)) {
                    change = true;
                }

                // if subscribe data is changed, or consumer is died too long.
                if (change) {
                    // 如果过滤器数据发生变化，创建新的过滤器数据对象，并更新到groupFilterData中
                    ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                    if (consumerFilterData == null) {
                        // new expression compile error, remove old, let client report error.
                        this.groupFilterData.remove(consumerGroup);
                        return false;
                    }
                    consumerFilterData.setBloomFilterData(bloomFilterData);

                    this.groupFilterData.put(consumerGroup, consumerFilterData);

                    log.info("Consumer filter info change, old: {}, new: {}, change: {}",
                        old, consumerFilterData, change);

                    return true;
                } else {
                    old.setClientVersion(clientVersion);
                    if (old.isDead()) {
                        reAlive(old);
                    }
                    return true;
                }
            }
        }

        /**
         * 重新激活消费者过滤器数据
         * @param filterData
         */
        protected void reAlive(ConsumerFilterData filterData) {
            // 获取原来的过期时间
            long oldDeadTime = filterData.getDeadTime();
            // 设置过期时间为0，表示不再过期，重新激活
            filterData.setDeadTime(0);
            log.info("Re alive consumer filter: {}, oldDeadTime: {}", filterData, oldDeadTime);
        }

        public final ConsumerFilterData get(String consumerGroup) {
            return this.groupFilterData.get(consumerGroup);
        }

        public final ConcurrentMap<String, ConsumerFilterData> getGroupFilterData() {
            return this.groupFilterData;
        }

        public void setGroupFilterData(final ConcurrentHashMap<String, ConsumerFilterData> groupFilterData) {
            this.groupFilterData = groupFilterData;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(final String topic) {
            this.topic = topic;
        }
    }
}
