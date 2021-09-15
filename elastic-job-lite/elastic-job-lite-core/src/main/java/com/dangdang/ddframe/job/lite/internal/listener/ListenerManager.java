/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.lite.internal.listener;

import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.internal.config.RescheduleListenerManager;
import com.dangdang.ddframe.job.lite.internal.election.ElectionListenerManager;
import com.dangdang.ddframe.job.lite.internal.failover.FailoverListenerManager;
import com.dangdang.ddframe.job.lite.internal.guarantee.GuaranteeListenerManager;
import com.dangdang.ddframe.job.lite.internal.instance.ShutdownListenerManager;
import com.dangdang.ddframe.job.lite.internal.instance.TriggerListenerManager;
import com.dangdang.ddframe.job.lite.internal.sharding.MonitorExecutionListenerManager;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingListenerManager;
import com.dangdang.ddframe.job.lite.internal.storage.JobNodeStorage;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;

import java.util.List;

/**
 * 作业注册中心的监听器管理者.
 *
 * @author zhangliang
 */
public final class ListenerManager {

    private final JobNodeStorage jobNodeStorage;

    private final ElectionListenerManager electionListenerManager;

    private final ShardingListenerManager shardingListenerManager;

    private final FailoverListenerManager failoverListenerManager;

    private final MonitorExecutionListenerManager monitorExecutionListenerManager;

    private final ShutdownListenerManager shutdownListenerManager;

    private final TriggerListenerManager triggerListenerManager;

    private final RescheduleListenerManager rescheduleListenerManager;

    private final GuaranteeListenerManager guaranteeListenerManager;

    private final RegistryCenterConnectionStateListener regCenterConnectionStateListener;

    public ListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName, final List<ElasticJobListener> elasticJobListeners) {
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        electionListenerManager = new ElectionListenerManager(regCenter, jobName);
        shardingListenerManager = new ShardingListenerManager(regCenter, jobName);
        failoverListenerManager = new FailoverListenerManager(regCenter, jobName);
        monitorExecutionListenerManager = new MonitorExecutionListenerManager(regCenter, jobName);
        shutdownListenerManager = new ShutdownListenerManager(regCenter, jobName);
        triggerListenerManager = new TriggerListenerManager(regCenter, jobName);
        rescheduleListenerManager = new RescheduleListenerManager(regCenter, jobName);
        guaranteeListenerManager = new GuaranteeListenerManager(regCenter, jobName, elasticJobListeners);
        regCenterConnectionStateListener = new RegistryCenterConnectionStateListener(regCenter, jobName);
    }

    /**
     * 开启所有监听器.
     */
    public void startAllListeners() {
        // 主节点选举监听管理器
        electionListenerManager.start();
        // 分片监听管理器
        shardingListenerManager.start();
        // 失效转移监听管理器
        failoverListenerManager.start();
        // 幂等性监听管理器
        monitorExecutionListenerManager.start();
        // 运行实例关闭监听管理器
        shutdownListenerManager.start();
        // 作业触发监听管理器
        triggerListenerManager.start();
        // 重调度监听管理器
        rescheduleListenerManager.start();
        // 保证分布式任务全部开始和结束状态监听管理器
        guaranteeListenerManager.start();
        // 注册中心连接状态监听器
        jobNodeStorage.addConnectionStateListener(regCenterConnectionStateListener);
    }
}
