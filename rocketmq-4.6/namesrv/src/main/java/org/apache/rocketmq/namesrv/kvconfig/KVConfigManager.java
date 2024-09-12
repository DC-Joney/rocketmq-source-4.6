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
package org.apache.rocketmq.namesrv.kvconfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.namesrv.NamesrvController;

// KV 配置管理器，在内存中维护所有的kv配置列表，并通过一个读写锁解决并发同步问题。
public class KVConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvController namesrvController;

    // 在 KVConfigManager 配置管理组件中，使用到 JDK 并发包中的 ReadWriteLock读写锁，实现对内存数据结构 configTable 的并发控制
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // 配置 Hash 表，外层的 Namespace 表示的是 Topic 维度，内层的 kv 为就是配置的 key/value

    // 维护了一个 configTable 的 HashMap 数据结构用于存储 KV 配置，
    // Key 是 Namespace 命名空间，Value 是一个子 HashMap 数据结构，用户存储每个 Namespace 命名空间下的子 KV 配置。
    private final HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable =
        new HashMap<String, HashMap<String, String>>();

    public KVConfigManager(NamesrvController namesrvController) {
        // 构造方法：传入一个namesrvController为内部的controller属性赋值
        this.namesrvController = namesrvController;
    }

    public void load() {
        String content = null;
        try {
            // 加载 kvConfigPath 下 kvConfig.json 配置文件里的 KV 配置，然后将这些配置放到KVConfigManager#configTable 属性中，
            // KVConfig 配置文件默认路径是 ${user.home}/namesrv/kvConfig.json
            content = MixAll.file2String(this.namesrvController.getNamesrvConfig().getKvConfigPath());
        } catch (IOException e) {
            log.warn("Load KV config table exception", e);
        }
        // 将 json 转换为对象类型
        if (content != null) {
            KVConfigSerializeWrapper kvConfigSerializeWrapper =
                KVConfigSerializeWrapper.fromJson(content, KVConfigSerializeWrapper.class);
            // 最后将 KV 变量放入配置表
            if (null != kvConfigSerializeWrapper) {
                this.configTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                log.info("load KV config table OK");
            }
        }
    }

    public void putKVConfig(final String namespace, final String key, final String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                // 根据命名空间获取该空间下的所有 KV 配置
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                // 如果该空间下还没有 KV，开始初始化
                if (null == kvTable) {
                    kvTable = new HashMap<String, String>();
                    this.configTable.put(namespace, kvTable);
                    log.info("putKVConfig create new Namespace {}", namespace);
                }

                final String prev = kvTable.put(key, value);
                if (null != prev) {
                    log.info("putKVConfig update config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                } else {
                    log.info("putKVConfig create new config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putKVConfig InterruptedException", e);
        }

        this.persist();
    }

    public void persist() {
        try {
            // 获取写锁，直到被其他线程中断
            this.lock.readLock().lockInterruptibly();
            try {

                // 新建一个 KV 封装器对象
                KVConfigSerializeWrapper kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
                // 将配置表 configTable 放入 KV 封装器对象
                kvConfigSerializeWrapper.setConfigTable(this.configTable);
                // 转换为 json 对象
                String content = kvConfigSerializeWrapper.toJson();

                if (null != content) {
                    // 持久化磁盘逻辑如下：
                    // 1、用内存中的 configTable 生成一个临时文件 tmp，同时把旧的 kvConfig.json 中的内容读取出来，写入到一个 bak 的备份文件
                    // 2、把旧的 kvConfig.json 文件删除
                    // 3、把最新的 tmp 文件重名回 kvConfig.json
                    MixAll.string2File(content, this.namesrvController.getNamesrvConfig().getKvConfigPath());
                }
            } catch (IOException e) {
                log.error("persist kvconfig Exception, "
                    + this.namesrvController.getNamesrvConfig().getKvConfigPath(), e);
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("persist InterruptedException", e);
        }

    }

    public void deleteKVConfig(final String namespace, final String key) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {

                // 根据命名空间获取该空间下的所有 KV 配置
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    // 如果不为空，就从 HashMap 中移除目标 KV
                    String value = kvTable.remove(key);
                    log.info("deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}",
                        namespace, key, value);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("deleteKVConfig InterruptedException", e);
        }

        this.persist();
    }

    /**
     * 获取指定命名空间下的 KV 配置
     * @param namespace namespace
     */
    public byte[] getKVListByNamespace(final String namespace) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    KVTable table = new KVTable();
                    table.setTable(kvTable);
                    return table.encode();
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVListByNamespace InterruptedException", e);
        }

        return null;
    }

    /**
     * 获取指定命名空间下指定 key 的 value 配置
     * @param namespace namespace
     * @param key 获取的key
     */
    public String getKVConfig(final String namespace, final String key) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    return kvTable.get(key);
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getKVConfig InterruptedException", e);
        }

        return null;
    }

    public void printAllPeriodically() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                log.info("--------------------------------------------------------");

                {
                    log.info("configTable SIZE: {}", this.configTable.size());
                    Iterator<Entry<String, HashMap<String, String>>> it =
                        this.configTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, HashMap<String, String>> next = it.next();
                        Iterator<Entry<String, String>> itSub = next.getValue().entrySet().iterator();
                        while (itSub.hasNext()) {
                            Entry<String, String> nextSub = itSub.next();
                            log.info("configTable NS: {} Key: {} Value: {}", next.getKey(), nextSub.getKey(),
                                nextSub.getValue());
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("printAllPeriodically InterruptedException", e);
        }
    }
}
