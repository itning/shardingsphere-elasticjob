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

package com.dangdang.ddframe.job.lite.internal.failover;

import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationNode;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.config.LiteJobConfigurationGsonFactory;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceNode;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

import java.util.List;

/**
 * 失效转移监听管理器.
 *
 * @author zhangliang
 */
public final class FailoverListenerManager extends AbstractListenerManager {

    private final String jobName;

    private final ConfigurationService configService;

    private final ShardingService shardingService;

    private final FailoverService failoverService;

    private final ConfigurationNode configNode;

    private final InstanceNode instanceNode;

    public FailoverListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        failoverService = new FailoverService(regCenter, jobName);
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
    }

    @Override
    public void start() {
        addDataListener(new JobCrashedJobListener());
        addDataListener(new FailoverSettingsChangedJobListener());
    }

    private boolean isFailoverEnabled() {
        LiteJobConfiguration jobConfig = configService.load(true);
        return null != jobConfig && jobConfig.isFailover();
    }

    /**
     * 失效转移
     */
    class JobCrashedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            // 失效转移开启 && 事件类型为移除 && 实例节点
            if (isFailoverEnabled() && Type.NODE_REMOVED == eventType && instanceNode.isInstancePath(path)) {
                String jobInstanceId = path.substring(instanceNode.getInstanceFullPath().length() + 1);
                if (jobInstanceId.equals(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId())) {
                    return;
                }
                // 获取作业服务器的失效转移分片项集合
                List<Integer> failoverItems = failoverService.getFailoverItems(jobInstanceId);
                if (!failoverItems.isEmpty()) {
                    // 集合不为空
                    for (int each : failoverItems) {
                        // 设置失效的分片项标记
                        failoverService.setCrashedFailoverFlag(each);
                        // 如果需要失效转移, 则执行作业失效转移
                        failoverService.failoverIfNecessary();
                    }
                } else {
                    for (int each : shardingService.getShardingItems(jobInstanceId)) {
                        failoverService.setCrashedFailoverFlag(each);
                        failoverService.failoverIfNecessary();
                    }
                }
            }
        }
    }

    /**
     * 失效转移设置变更监听器
     */
    class FailoverSettingsChangedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            // 如果关闭了失效转移
            if (configNode.isConfigPath(path) && Type.NODE_UPDATED == eventType && !LiteJobConfigurationGsonFactory.fromJson(data).isFailover()) {
                // 删除作业失效转移信息
                failoverService.removeFailoverInfo();
            }
        }
    }
}
