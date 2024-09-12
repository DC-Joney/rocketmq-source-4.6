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
package org.apache.rocketmq.broker.client.net;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

public class Broker2Client {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public Broker2Client(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 检查生产者事务状态，该方法主要是跟事务消息结合来使用的
     * 如果生产者发送的是事务消息，那么此时先发送 half 消息，成功执行 commit，否则执行 rollback
     * 如果连接中断，broker 会检查事务消息的状态，以及回调生产者客户端检查本地事务状态，通过 broker 主动回查来确认是 commit 还是 rollback
     *
     * 这里我们来简单说下 RocketMQ 的事务消息原理：
     * 1、生产者先发送一个 half 消息，如果 half 消息成功了，此时就可以执行本地事务
     * 2、如果本地事务成功，则提交消息，否则回滚消息。
     * 3、如果 half 消息失败，或者提交/回滚消息没有发送成功，此时 broker 会自动回查 half 消息状态
     * 4、如果 half 消息超时了一直没有提交/回滚，此时会回查生产者，检查本地事务提交成功/失败，此时 broker 再决定 half 消息是提交/回滚
     * @param group  生产者组
     * @param channel 生产者网络连接
     * @param requestHeader 检查事务状态请求头
     * @param messageExt 消息扩展数据
     */
    public void checkProducerTransactionState(
        final String group,
        final Channel channel,
        final CheckTransactionStateRequestHeader requestHeader,
        final MessageExt messageExt) throws Exception {

        // 构造检查生产者本地事务状态请求
        // 当 half 消息超时后还没有提交/回滚，即没收到生产者发送过来的本地事务状态，此时需要构造请求去回查
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
        // 将消息编码后设置到请求体中
        request.setBody(MessageDecoder.encode(messageExt, false));
        try {
            // 通过网络通信服务器向生产者发送检查事务状态请求，单向发送模式进行发送，超时时间 10 毫秒
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("Check transaction failed because invoke producer exception. group={}, msgId={}", group, messageExt.getMsgId(), e.getMessage());
        }
    }

    // 便捷方法，直接把构造好的 RemotingCommand 通过 netty 网络连接发送出去
    public RemotingCommand callClient(final Channel channel,
                                      final RemotingCommand request
    ) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {

        // 调用客户端，发送同步请求，超时时间 10 秒
        return this.brokerController.getRemotingServer().invokeSync(channel, request, 10000);
    }


    // 通知客户端消费者组信息发生变化，当客户端收到消息后触发rebalence
    public void notifyConsumerIdsChanged(
        final Channel channel,
        final String consumerGroup) {
        if (null == consumerGroup) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        // 构造通知消费者组信息发生变化请求
        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);

        try {

            // 通过网络通信服务器向客户端发送通知消费者组信息发生变化请求，单向发送模式进行发送，超时时间 10 毫秒
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e.getMessage());
        }
    }

    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce) {
        return resetOffset(topic, group, timeStamp, isForce, false);
    }

    /**
     * 重置消费组下所有消费者的消费偏移量
     * @param topic 主题
     * @param group 消费者组
     * @param timeStamp 时间戳
     * @param isForce 是否强制
     * @param isC 是否 c 语言
     * @return
     */
    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce,
                                       boolean isC) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        //获取topic的元数据信息
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            log.error("[reset-offset] reset offset failed, no topic in this broker. topic={}", topic);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic);
            return response;
        }


        //用于存放每个MessageQueue中对应的offset信息
        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();

            //设置broker信息
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setTopic(topic);
            mq.setQueueId(i);

            //获取当前topic的write queue的offset信息
            long consumerOffset =
                this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, i);
            if (-1 == consumerOffset) {
                // 获取不到消费偏移量，则返回系统异常  错误响应
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("THe consumer group <%s> not exist", group));
                return response;
            }

            long timeStampOffset;

            // 从消息存储组件，查询最大偏移量/根据时间戳获取偏移量
            if (timeStamp == -1) {
                // 查询最大偏移量
                timeStampOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            } else {
                // 根据时间戳查找对应的偏移量信息
                timeStampOffset = this.brokerController.getMessageStore().getOffsetInQueueByTime(topic, i, timeStamp);
            }

            if (timeStampOffset < 0) {
                log.warn("reset offset is invalid. topic={}, queueId={}, timeStampOffset={}", topic, i, timeStampOffset);
                timeStampOffset = 0;
            }

            // 是否强制启用 或者 时间戳偏移量是否小于消费偏移量
            if (isForce || timeStampOffset < consumerOffset) {
                // 设置时间戳偏移量到 offsetTable
                offsetTable.put(mq, timeStampOffset);
            } else {
                // 设置消费偏移量到 offsetTable
                offsetTable.put(mq, consumerOffset);
            }
        }

        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timeStamp);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, requestHeader);
        if (isC) {
            // c++ language

            ResetOffsetBodyForC body = new ResetOffsetBodyForC();
            List<MessageQueueForC> offsetList = convertOffsetTable2OffsetList(offsetTable);
            body.setOffsetTable(offsetList);
            request.setBody(body.encode());
        } else {
            // other language
            // 构建重置消费偏移量请求头
            ResetOffsetBody body = new ResetOffsetBody();
            //将messageQueue以及对应的offset存放到offsetTable发送到客户端
            body.setOffsetTable(offsetTable);
            request.setBody(body.encode());
        }

        //通过group name 获取当前消费者组信息
        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(group);

        //遍历当前消费者组中所有的消费者，并且将请求下发给所有的消费者
        if (consumerGroupInfo != null && !consumerGroupInfo.getAllChannel().isEmpty()) {
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            //获取消费者组下的所有消费者对应的channel
            for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
                int version = entry.getValue().getVersion();
                if (version >= MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                    try {

                        // 通过网络通信服务器向客户端发送重置消费偏移量请求，单向发送，超时时间 5 秒
                        this.brokerController.getRemotingServer().invokeOneway(entry.getKey(), request, 5000);
                        log.info("[reset-offset] reset offset success. topic={}, group={}, clientId={}",
                            topic, group, entry.getValue().getClientId());
                    } catch (Exception e) {
                        log.error("[reset-offset] reset offset exception. topic={}, group={}",
                            new Object[] {topic, group}, e);
                    }
                } else {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("the client does not support this feature. version="
                        + MQVersion.getVersionDesc(version));
                    log.warn("[reset-offset] the client does not support this feature. version={}",
                        RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                    return response;
                }
            }
        } else {
            String errorInfo =
                String.format("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d",
                    requestHeader.getGroup(),
                    requestHeader.getTopic(),
                    requestHeader.getTimestamp());
            log.error(errorInfo);
            response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
            response.setRemark(errorInfo);
            return response;
        }

        // 发送成功响应
        response.setCode(ResponseCode.SUCCESS);
        ResetOffsetBody resBody = new ResetOffsetBody();
        // 重新设置 offsetTable 到请求体中
        resBody.setOffsetTable(offsetTable);
        response.setBody(resBody.encode());
        return response;
    }

    private List<MessageQueueForC> convertOffsetTable2OffsetList(Map<MessageQueue, Long> table) {
        List<MessageQueueForC> list = new ArrayList<>();
        for (Entry<MessageQueue, Long> entry : table.entrySet()) {
            MessageQueue mq = entry.getKey();
            MessageQueueForC tmp =
                new MessageQueueForC(mq.getTopic(), mq.getBrokerName(), mq.getQueueId(), entry.getValue());
            list.add(tmp);
        }
        return list;
    }

    /**
     * 获取消费者状态,指定客户端 id 对应的 topic 下的 group 的消费状态
     * @param topic topic
     * @param group 消费组
     * @param originClientId 获取某一个消费者对应的offset信息
     * @return
     */
    public RemotingCommand getConsumeStatus(String topic, String group, String originClientId) {
        final RemotingCommand result = RemotingCommand.createResponseCommand(null);

        // 构建消费者状态请求头
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        // 设置 topic
        requestHeader.setTopic(topic);
        // 设置消费者组
        requestHeader.setGroup(group);
        // 构建获取消费者状态请求
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT,
                requestHeader);

        // 初始化 consumerStatusTable，用于存放consumer对应的状态信息
        Map<String, Map<MessageQueue, Long>> consumerStatusTable =
            new HashMap<String, Map<MessageQueue, Long>>();

        //获取消费者组的所有消费者
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(group).getChannelInfoTable();
        if (null == channelInfoTable || channelInfoTable.isEmpty()) {
            result.setCode(ResponseCode.SYSTEM_ERROR);
            result.setRemark(String.format("No Any Consumer online in the consumer group: [%s]", group));
            return result;
        }

        //遍历所有消费者
        for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
            int version = entry.getValue().getVersion();
            String clientId = entry.getValue().getClientId();
            if (version < MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                result.setCode(ResponseCode.SYSTEM_ERROR);
                result.setRemark("the client does not support this feature. version="
                    + MQVersion.getVersionDesc(version));
                log.warn("[get-consumer-status] the client does not support this feature. version={}",
                    RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                return result;
            }
            //如果消费者的id与传入的id相同则是需要发送状态的消费者
            else if (UtilAll.isBlank(originClientId) || originClientId.equals(clientId)) {
                try {

                    // 通过网络通信服务器向客户端发送获取消费者状态请求，发起同步请求，超时时间 5 秒
                    RemotingCommand response =
                        this.brokerController.getRemotingServer().invokeSync(entry.getKey(), request, 5000);
                    assert response != null;
                    switch (response.getCode()) {
                        case ResponseCode.SUCCESS: {
                            if (response.getBody() != null) {
                                GetConsumerStatusBody body =
                                    GetConsumerStatusBody.decode(response.getBody(),
                                        GetConsumerStatusBody.class);

                                // 请求成功，获取消费者状态，存入消费者状态 table
                                consumerStatusTable.put(clientId, body.getMessageQueueTable());
                                log.info(
                                    "[get-consumer-status] get consumer status success. topic={}, group={}, channelRemoteAddr={}",
                                    topic, group, clientId);
                            }
                        }
                        default:
                            break;
                    }
                } catch (Exception e) {
                    log.error(
                        "[get-consumer-status] get consumer status exception. topic={}, group={}, offset={}",
                        new Object[] {topic, group}, e);
                }

                if (!UtilAll.isBlank(originClientId) && originClientId.equals(clientId)) {
                    break;
                }
            }
        }

        // 构建获取消费者状态响应
        result.setCode(ResponseCode.SUCCESS);
        GetConsumerStatusBody resBody = new GetConsumerStatusBody();
        resBody.setConsumerTable(consumerStatusTable);
        result.setBody(resBody.encode());
        return result;
    }
}
