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

package org.apache.rocketmq.client.latency;

public interface LatencyFaultTolerance<T> {
    //更新故障项
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    //判断某个故障项，是否已经可用
    boolean isAvailable(final T name);

    //移除某个故障项
    void remove(final T name);

    // 那么就从 faultItemTable 故障列表取出一个Broker即可
    //按照延迟时间或者故障时间排序，找出最小的那个broker
    T pickOneAtLeast();
}
