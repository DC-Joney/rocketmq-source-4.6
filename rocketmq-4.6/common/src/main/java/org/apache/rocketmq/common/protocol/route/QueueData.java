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

/**
 * $Id: QueueData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.route;

public class QueueData implements Comparable<QueueData> {
    // 每个 queue 都属于一个数据分区，一定是在一个 broker 组里，所以这里要指定 broker 名称，代表了当前的 broker 组
    private String brokerName;
    // 读队列数量
    private int readQueueNums;
    // 写队列数量
    private int writeQueueNums;
    //队列的读写权限
    /**
     * 在 broker 里，假如该 topic 有 4 个 write queue，4 个 read queue。随机的从 4 个 write queue里获取到一个 queue 来写入数据，在消费的时候，从 4 个 read queue 里随机的挑选一个，来读取数据。
     * 这里举 2 个 例子，如下：
     * （1）、4 个 write queue，2 个 read queue -> 会均匀的写入到 4 个 write queue 里去，读数据的时候仅仅会读里面的 2 个 queue 的数据
     * （2）、4 个 write queue，8个 read queue -> 此时只会写入 4 个queue里，但是消费的时候随机从 8 个queue里消费的
     * 所以区分读写队列作用是帮助我们对 topic 的 queues 进行扩容和缩容，8 个 write queue + 8 个 read queue
     *  4 个 write queue -> 写入数据仅仅会进入这 4 个 write queue 里去
     *  8 个 read queue，读取数据，有 4 个 queue 持续消费到最新的数据，另外 4 个 queue 不会写入新数据，但是会把他已有的数据全部消费完毕，最后 8 个 read queue -> 4 个 read queue
     */

    // 设置该 Topic 的读写模式 6：同时支持读写  4：禁写   2：禁读  一般情况设置为: 6
    private int perm;
    //队列的同步标志
    private int topicSynFlag;

    public int getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(int readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public int getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(int writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public int getPerm() {
        return perm;
    }

    public void setPerm(int perm) {
        this.perm = perm;
    }

    public int getTopicSynFlag() {
        return topicSynFlag;
    }

    public void setTopicSynFlag(int topicSynFlag) {
        this.topicSynFlag = topicSynFlag;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + perm;
        result = prime * result + readQueueNums;
        result = prime * result + writeQueueNums;
        result = prime * result + topicSynFlag;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueueData other = (QueueData) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (perm != other.perm)
            return false;
        if (readQueueNums != other.readQueueNums)
            return false;
        if (writeQueueNums != other.writeQueueNums)
            return false;
        if (topicSynFlag != other.topicSynFlag)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + readQueueNums
            + ", writeQueueNums=" + writeQueueNums + ", perm=" + perm + ", topicSynFlag=" + topicSynFlag
            + "]";
    }

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
