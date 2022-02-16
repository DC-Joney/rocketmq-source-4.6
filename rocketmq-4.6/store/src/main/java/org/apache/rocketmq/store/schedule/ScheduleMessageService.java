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
package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;  //10s

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
            new ConcurrentHashMap<Integer, Long>(32);

    //存储在 delayOffset.json 消费进度偏移
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
            new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private Timer timer;
    private MessageStore writeMessageStore;
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }


    public void start() {

        //设置运行状态为开始
        if (started.compareAndSet(false, true)) {
            this.timer = new Timer("ScheduleMessageTimerThread", true);

            // 遍历延时级别map，为每一个延时级别建立一个DeliverDelayedMessageTimerTask定时任务，
            // 并设置指定offset，第一次调度延时为FIRST_DELAY_TIME，即1s

            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {

                //获取等级和 延迟时间长度
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                //获取对应延迟级别的 消息 偏移量缓存，这里缓存的是ConsumeQueue文件中的消息的偏移量
                Long offset = this.offsetTable.get(level);
                //如果偏移量为null，说明没有消息需要处理，这设置为0
                //offset 消费进度偏移
                if (null == offset) {
                    offset = 0L;
                }

                //如果延迟级别不为null，则构建DeliverDelayedMessageTimerTask任务进行处理
                //为每一个延时级别建立一个DeliverDelayedMessageTimerTask定时任务，
                // 并设置指定offset，第一次调度延时为FIRST_DELAY_TIME，即1s
                if (timeDelay != null) {
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }

            // 每隔10秒持久化延时消息消费进度
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) ScheduleMessageService.this.persist();//持久化
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    public boolean load() {
        //调用父类的加载文件的方法，父类会调用子类实现的configFilePath方法确定文件
        boolean result = super.load();
        //加载成功则进行解析延迟级别配置
        result = result && this.parseDelayLevel();
        return result;
    }

    @Override
    public String configFilePath() {

        //获取 delayOffset.json 文件
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                    DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    //解析MessageStoreConfig中messageDelayLevel的定义，并转换成delayLevelTable
    public boolean parseDelayLevel() {
        //不同延迟级别的基础时间长度，单位为毫秒
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);
//获取延迟配置
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {

            //进行分割解析，分割符号为空格
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);

                //获取对应的延迟毫秒数
                Long tu = timeUnitTable.get(ch);

                //寻找最大的延迟等级
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                //获取延迟的时长
                long num = Long.parseLong(value.substring(0, value.length() - 1));

                //计算真正的时长
                long delayTimeMillis = tu * num;

                //保存到延迟级别缓存中
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    class DeliverDelayedMessageTimerTask extends TimerTask {
        private final int delayLevel;
        private final long offset; //offset 消费进度偏移

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    //到点执行，开始是1s
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                        this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            //最大的迁移时间戳，为now+ 队列的周期，比如 10s
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            //如果  迁移时间戳  已经大于  现在的时间 + 周期， 那么现在就扫描
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        public void executeOnTimeup() {

            //根据 SCHEDULE_TOPIC 和 延迟级别 找到对应的ConsumeQueue
            ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC, delayLevel2QueueId(delayLevel));

            //offset 消费进度偏移
            long failScheduleOffset = offset;

            if (cq != null) {
                //根据传入的  offset  的从ConsumeQueue中获取对应的  文件映射
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        //获取额外的信息单元
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            //获取消息在CommitLog中的真实位置
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            //获取消息的大小
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            //消息对应的tag的hashcode
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            //然后根据Offset从CommitLog中获取消息的落盘时间
                            //如果额外的信息不为空，则获取
                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                            tagsCode, offsetPy, sizePy);

                                    //从CommitLog中获取消息的存储时间

                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            //计算当前时间和落盘时间的时间差，检查延迟时间是否到了
                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            //下一个消息单元的偏移量
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            long countdown = deliverTimestamp - now;

                            //消息的延迟时间到了
                            if (countdown <= 0) {
                                MessageExt msgExt =
                                        ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);

                                //锁定消息
                                if (msgExt != null) {
                                    try {
                                        //构建真正的消息，把真实的Topic和QueueId恢复
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);

                                        //把消息保存到CommitLog，等待消费
                                        PutMessageResult putMessageResult =
                                                ScheduleMessageService.this.writeMessageStore
                                                        .putMessage(msgInner);

                                        //保存成功，则继续处理下一条消息
                                        if (putMessageResult != null
                                                && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                    "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                    msgExt.getTopic(), msgExt.getMsgId());

                                            //如果保存失败，则创建新的延迟任务。10秒钟之后重试
                                            ScheduleMessageService.this.timer.schedule(
                                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        log.error(
                                                "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                        + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                        + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                //如果没有到期
                                //重新构建DeliverDelayedMessageTimerTask，等待下个时间段在来扫描
                                ScheduleMessageService.this.timer.schedule(
                                        new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                        countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                                this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                                + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                    failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                    MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
