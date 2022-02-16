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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.console.task;

import org.apache.rocketmq.console.model.ConsumerMonitorConfig;
import org.apache.rocketmq.console.model.GroupConsumeInfo;
import org.apache.rocketmq.console.service.ConsumerService;
import org.apache.rocketmq.console.service.MonitorService;
import org.apache.rocketmq.console.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

@Component
public class MonitorTask {
    private Logger logger = LoggerFactory.getLogger(MonitorTask.class);

    @Resource
    private MonitorService monitorService;

    @Resource
    private ConsumerService consumerService;

    /*
    //    @Scheduled(cron = "* * * * * ?")
        public void scanProblemConsumeGroup() {
            for (Map.Entry<String, ConsumerMonitorConfig> configEntry : monitorService.queryConsumerMonitorConfig().entrySet()) {
                GroupConsumeInfo consumeInfo = consumerService.queryGroup(configEntry.getKey());
                if (consumeInfo.getCount() < configEntry.getValue().getMinCount() || consumeInfo.getDiffTotal() > configEntry.getValue().getMaxDiffTotal()) {
                    logger.info("op=look consumeInfo {}", JsonUtil.obj2String(consumeInfo)); // notify the alert system
                }
            }
        }*/
    @Scheduled(cron = "* * * * * ?")
    public void scanProblemConsumeGroup() {
        for (Map.Entry<String, ConsumerMonitorConfig> configEntry : monitorService.queryConsumerMonitorConfig().entrySet()) {

            GroupConsumeInfo consumeInfo = null;
            try {
                consumeInfo = consumerService.queryGroup(configEntry.getKey());

            } catch (Exception e) {
                logger.error("读取消费组发生异常 {}", e.getMessage());

            }

            if (null == consumeInfo) {
                continue;
            }
            if (consumeInfo.getCount() < configEntry.getValue().getMinCount() || consumeInfo.getDiffTotal() > configEntry.getValue().getMaxDiffTotal()) {
                logger.error("op=look consumeInfo {}", JsonUtil.obj2String(consumeInfo));
                //to  notify the alert system
            }
        }
    }
}
