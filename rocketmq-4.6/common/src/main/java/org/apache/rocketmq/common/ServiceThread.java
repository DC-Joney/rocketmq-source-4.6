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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

// 1 可以执行周期性任务
// 2 可以执行紧急任务
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    // 线程加入时间，90秒
    private static final long JOIN_TIME = 90 * 1000;

    // 线程对象
    private Thread thread;

    // 用于线程同步的CountDownLatch
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    // 用于线程间通知的原子布尔变量
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    // 用于标识线程是否停止
    protected volatile boolean stopped = false;

    // 标识线程是否为守护线程
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    // 用于标识线程是否已经启动，可以用于重启线程
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 只有当线程还未启动时，才执行下面的逻辑
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // 设置stopped为false，表示线程未停止
        stopped = false;
        // 创建一个新的线程
        this.thread = new Thread(this, getServiceName());
        // 设置线程是否为守护线程
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    public void wakeup() {
        // 设置通知状态为 true， 前提是之前为 false
        if (hasNotified.compareAndSet(false, true)) {

            //发通知
            waitPoint.countDown(); // notify
        }
    }

    protected void waitForRunning(long interval) {

        // 如果 hasNotified 为 true， 表示有数据过来了， 不用等了
        // 提前结束等待执行onWaitEnd()方法
        // 比如： 交换读写 swapRequests（） ,刷盘请求的requestsWrite->requestsRead

        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        // 如果 hasNotified 为 false，标志着：还没有数据过来， 等

        //entry to wait
        waitPoint.reset();

        //没有数据过来， 等待， 比如10ms
        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
