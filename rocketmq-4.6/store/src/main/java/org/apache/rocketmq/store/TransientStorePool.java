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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;


// TransientStorePool：临时存储池。
// RocketMQ单独创建一个MappedByteBuffer内存映射，用来临时存储数据，
// 数据先写入该内存映射中，然后由commit线程定时将数据从该内存复制到与目的物理文件对应的内存映射中。
// RokcetMQ引入该机制主要的原因是提供一种内存锁定机制，
// 将当前堆外内存一直锁定在内存中，避免被进程将内存交换到磁盘。

public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);


    // 内存池大小 5个
    private final int poolSize;

    // 单个 ByteBuffer 文件大小 1G
    private final int fileSize;

    // 可用 buffer 队列
    private final Deque<ByteBuffer> availableBuffers;

    // 消息存储类
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        //默认是1个G，这个很猛哦
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        //默认是5个大内存，一个内存1个G， 也就是临时存储池，需要耗费5个G的内存
        for (int i = 0; i < poolSize; i++) {

            // ByteBuffer 文件大小 1G
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            // 如果是 mappedByteBuffer 类型，可以通过以下方式获取直接内存的地址
            final long address = ((DirectBuffer) byteBuffer).address();

            // 创建指针指向直接内存的地址
            Pointer pointer = new Pointer(address);

            //创建 poolSize 个堆外内存，并利用 com.sun.jna.Library 类库的 mlock 系统调用将该批内存锁定，避免被置换到交换区，提高存储性能。
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            // 将 ByteBuffer 放入可用的 ByteBuffer 队列中
            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁内存：对之前初始化时所分配并锁定的直接内存进行解锁和释放
     */
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);

            // 利用 com.sun.jna.Library 类库的 munlock 系统调用解锁并释放内存
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 归还 ByteBuffer
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        // 设置ByteBuffer的位置和限制，将它们重置为初始状态，以便重新使用
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);

        // 将ByteBuffer放回队列的头部，以备后续使用
        // 这种做法可以提高内存的复用性和减少内存重分配的开销
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        // 从队列头部取出一个ByteBuffer
        ByteBuffer buffer = availableBuffers.pollFirst();

        // 如果可用ByteBuffer的数量少于池大小的40%，记录警告日志
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {

        // 如果启动，则返回可用的堆外内存池的数量
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
