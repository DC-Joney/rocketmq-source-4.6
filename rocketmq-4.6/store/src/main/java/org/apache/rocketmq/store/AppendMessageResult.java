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
package org.apache.rocketmq.store;

/**
 * When write a message to the commit log, returns results
 */
public class AppendMessageResult {
    // Return code
    private AppendMessageStatus status;
    // Where to start writing
    // 写入的物理偏移量。
    private long wroteOffset;
    // 写入字节总长度 Write Bytes

    private int wroteBytes;
    //  Message ID 消息id
    private String msgId;
    // Message storage timestamp 消息存储时间，也就是写入到 MappedFile 中的时间
    private long storeTimestamp;
    // 逻辑的 Consume queue偏移量  Consume queue's offset(step by one)
    private long logicsOffset;

    //写入到 MappedByteBuffer (将消息内容写入到内存映射文件中的时长)
    private long pagecacheRT = 0;

    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, "", 0, 0, 0);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId,
        long storeTimestamp, long logicsOffset, long pagecacheRT) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
    }

    public long getPagecacheRT() {
        return pagecacheRT;
    }

    public void setPagecacheRT(final long pagecacheRT) {
        this.pagecacheRT = pagecacheRT;
    }

    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getLogicsOffset() {
        return logicsOffset;
    }

    public void setLogicsOffset(long logicsOffset) {
        this.logicsOffset = logicsOffset;
    }

    public int getMsgNum() {
        return msgNum;
    }

    public void setMsgNum(int msgNum) {
        this.msgNum = msgNum;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" +
            "status=" + status +
            ", wroteOffset=" + wroteOffset +
            ", wroteBytes=" + wroteBytes +
            ", msgId='" + msgId + '\'' +
            ", storeTimestamp=" + storeTimestamp +
            ", logicsOffset=" + logicsOffset +
            ", pagecacheRT=" + pagecacheRT +
            ", msgNum=" + msgNum +
            '}';
    }
}
