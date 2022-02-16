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
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class PullConsumerDemo {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws InterruptedException, MQClientException {

        /*
         * Instantiate with specified consumer group name.
         */
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("pull-consumer");

        consumer.setNamesrvAddr("127.0.0.1:9876");

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        //consumer.setBrokerSuspendMaxTimeMillis(1000);
        System.out.println("ms:"+consumer.getBrokerSuspendMaxTimeMillis());
        consumer.start();
        //1.获取MessageQueues并遍历（一个Topic包括多个MessageQueue  默认4个）
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("topic-demo");
        for (MessageQueue mq : mqs) {
            System.out.println("queueID:"+ mq.getQueueId());
            //获取偏移量
            long Offset = consumer.fetchConsumeOffset(mq,true);
            System.out.printf("Consume from the queue: %s%n", mq);

            while (true) { //拉模式，必须无限循环
                try {
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq,null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n",pullResult);
                    //2.维护Offsetstore（这里存入一个Map）
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    //3.根据不同的消息状态做不同的处理
                    switch (pullResult.getPullStatus()) {
                        case FOUND: //获取到消息
                            for (int i=0;i<pullResult.getMsgFoundList().size();i++) {
                                System.out.printf("%s%n", new String(pullResult.getMsgFoundList().get(i).getBody()));
                            }
                            break;
                        case NO_MATCHED_MSG: //没有匹配的消息
                            break;
                        case NO_NEW_MSG:  //没有新消息
                            break;
                        case OFFSET_ILLEGAL: //非法偏移量
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        consumer.shutdown();
    }
    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }
    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }

}
