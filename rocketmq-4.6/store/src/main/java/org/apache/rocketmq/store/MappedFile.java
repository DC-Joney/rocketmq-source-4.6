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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

public class MappedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    //commitLog内存（ByteBuffer）写入位点，标记消息写到哪了，下次从该位置开始写。
     //在消息写完后递增，递增大小为消息的长度

    //当前MappedFile写入的位置
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    //当前MappedFile提交的位置,专用于二级缓存writeByteBuffer
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    //文件系统缓存里边的内容，刷入磁盘的位置
    //当前MappedFile刷入磁盘的位置
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    //文件大小
    protected int fileSize;
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;

    //用于分配写入缓存的的内存池
    protected TransientStorePool transientStorePool = null;

    //文件名称
    private String fileName;

    //fileFromOffset 可以是一个 mappedfile 文件的磁盘上起始的写入位置，因为每个 CommitLog 文件都是固定大小 1G，
    // 所以根据 fileFromOffset +mappedFileSize 计算出新文件的磁盘起始写入位置
    // fileFromOffset 是针对全局而言的
    private long fileFromOffset;
    private File file;

    //当前文件对应的MappedByteBuffer
    private MappedByteBuffer mappedByteBuffer;
    private volatile long storeTimestamp = 0;

    //当前文件是否对应于MappedFileQueue中的第一个文件
    //最新的MappedFile文件则是MappedFileQueue的第一个文件，既最近写入的文件
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    // 重点方法  追加消息
    // 将消息追加到MappedFile的MappedByteBuffer/writeBuffer中
    // 更新其写入位置wrotePosition
    // 但还没Commit及Flush
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;



        //获取当前内存对象的写入位置（wrotePostion变量值）；
        // 若写入位置没有超过文件大小则继续顺序写入；
        int currentPos = this.wrotePosition.get();

        //如果writePos小于文件的大小，说明还有空间可以写入数据
        if (currentPos < this.fileSize) {
            //由内存对象mappedByteBuffer创建一个指向同一块内存的ByteBuffer对象，并将内存对象的写入指针指向写入位置；
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            //将buffer的position设置为writePos
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            //以文件的起始偏移量（fileFromOffset）、ByteBuffer对象、该内存对象剩余的空间（fileSize-wrotePostion）、消息对象msg为参数调用AppendMessageCallback回调类的doAppend方法；
            if (messageExt instanceof MessageExtBrokerInner) {

                //将消息写入到byteBuffer中
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }

            //将MapedFile.wrotePostion的值加上写入的字节数（AppendMessageResult对象返回的值）；
            this.wrotePosition.addAndGet(result.getWroteBytes());

            //更新存储时间戳MapedFile.storeTimestamp
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        /*
         * 判断是否可以刷盘
         * 如果文件已经满了，或者如果 flushLeastPages 大于 0，且脏页数量大于等于 flushLeastPages
         * 或者如果 flushLeastPages 等于 0 并且存在脏数据，这几种情况都会进行刷盘
         */
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {

                //通过getReadPosition获取当前消息内容写入后的位置
                //因为在写入完消息之后就已经更新了readPosition，所以现在的readPosition = writeOffset + writeBytes
                int value = getReadPosition();

                try {

                    // 只将数据追加到 fileChannel 或 mappedByteBuffer 中，不会同时追加到这两个里面
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        // 如果使用了堆外内存，那么通过 fileChannel 强制刷盘，这是异步堆外内存执行的逻辑
                        this.fileChannel.force(false);
                    } else {

                        // 这里调用mappedByteBuffer的force方法，
                        // 通过JDK的NIO操作，将mappedByteBuffer缓存中的数据写入CommitLog文件中
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                // 设置新的刷新 ReadPosition
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        //返回新的刷新 ReadPosition
        return this.getFlushedPosition();
    }

    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }

        //如果提交的数据不满commitLeastPages则不执行本次的提交，待下一次提交
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        // 所有的脏数据被提交到了 FileChannel，那么归还堆外内存
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            // 将堆外内存重置，并存入内存 availableBuffer 的头部
            this.transientStorePool.returnBuffer(writeBuffer);
            // 将 writeBuffer 置为 null，下次再重新获取
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {

                //创建writeBuffer的共享缓存区
                ByteBuffer byteBuffer = writeBuffer.slice();
                //将指针回退到上一次提交的位置
                byteBuffer.position(lastCommittedPosition);

                //设置limit为writePos
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);

                //将committedPosition指针到wrotePosition的数据复制（写入）到fileChannel中
                this.fileChannel.write(byteBuffer);

                //更新committedPosition指针为writePos
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {

        //flush记录的是上一次完成刷新后的位置，
        int flush = this.flushedPosition.get();
        //write记录的是当前消息内容写入后的位置
        int write = getReadPosition();

        // 如果文件已经满了，那么返回 true
        if (this.isFull()) {
            return true;
        }

        //当flushLeastPages 大于0的时候，可以计算出是否满足page的要求，其中OS_PAGE_SIZE是4K，也就是说1个page大小是4k
        if (flushLeastPages > 0) {

            // 在异步刷盘中，flushLeastPages 是 4，
            // 也就是说，只有当缓存的消息至少是4（page个数）*4K（page大小）= 16K时，异步刷盘才会将缓存写入文件
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        //否则 flushLeastPages 为 0，那么只要写入位置大于刷盘位置，即存在脏数据就会进行刷盘
         return write > flush;
    }

    /**
     * 是否支持提交
     * 1. 首先获取提交位置 commit 和写入位置 write。
     * 2. 判断如果文件满了，即写入位置等于文件大小，那么直接返回 true。
     * 3. 如果至少提交的页数 commitLeastPages 大于 0，则需要比较写入位置与提交位置的差值，当差值大于指定的最少页数才可以进行提交，此举可以防止频繁提交。
     * 3. 否则 commitLeastPages 为 0，那么只要写入位置大于提交位置，即存在脏数据此时就一定会提交。
     * @param commitLeastPages 至少提交的页数
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        // 获取提交位置
        int flush = this.committedPosition.get();
        // 获取写入位置
        int write = this.wrotePosition.get();

        // 如果文件已经满了，那么返回 true
        if (this.isFull()) {
            return true;
        }

        //同步刷盘时flushLeastPages=0立刻刷盘,
        //异步刷盘时flushLeastPages=4 ,默认是4，需要刷盘的数据达到PageCache的页数4倍时才会刷盘，
        // 或者距上一次刷盘时间>=200ms则设置flushLeastPages=0立刻刷盘

        // 1 同步刷盘时无论消息的大小都立刻刷盘，线程阻塞等待刷盘结果
        // 2 异步刷盘有两种方式但是其逻辑都是需要刷盘的数据OS_PAGE_SIZE的4倍即（1024 * 4）*4=16k或者距上一次刷盘时间>=200ms时才刷盘，提高数据的刷盘性能

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        // 否则 commitLeastPages 为 0，那么只要写入位置大于提交位置，即存在脏数据就会进行提交
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    //读
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        //没有启用专用的写入缓存，二级缓存，返回写入的位置
        //否则，启用专用的写入缓存，二级缓存，返回已经提交的位置
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();

        //每次循环都会将i + MappedFile.OS_PAGE_SIZE
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {

            //在每个page页的首置位写入一个字节
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync

            //如果刷盘方式为同步刷盘，则将数据同步刷入到磁盘
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {

                    // Thread中sleep函数的作用是让该线程进入休眠状态，让出cpu的执行时间给其他进程，该线程休眠后进入就绪队列和其他线程一起竞争cpu的执行时间。
                    //
                    //　所以sleep(0)的作用就是让该线程立即从运行阶段进入就绪队列而非等待队列，释放cpu时间，
                    // 可以让操作系统切换其他线程来执行，提升效率。
                    // 总得来说就是，sleep(0)让当前已完成功能的线程让出自己的资源（时间片）给其他线程，
                    // “Thread.Sleep(0)作用,就是“触发操作系统立刻重新进行一次CPU竞争”。　让其他线程有竞争cpu资源的机会（该线程也在就绪队列参与竞争）

                    // 如果是 GC线程 获得CPU控制权 ， 就开始 进行GC处理， 清理其他引用 .
                    // Thread.sleep(0); 的 副作用是： 可能更频繁地运行GC
                    //  GC线程 获得CPU控制权 的好处：这可以防止 单次GC 操作 长时间运行,
                    // 比如这里没隔1000次迭代，就尝试GC运行，比一直都不让gc运行，然后让gc长时间运行效果更好 .

                    // 上面的注释，有歧义，准确来说，应该是 prevent long time  gc
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        //调用mlock方法将该内存锁住，防止被swap到swap分区
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {

            //锁住该内存
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {

            //调用 madvice函数，这里的参数是MADV_WILLNEED 作用是建议操作系统提前将这部分文件内容加载到物理内存，但是也可能不加载
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
