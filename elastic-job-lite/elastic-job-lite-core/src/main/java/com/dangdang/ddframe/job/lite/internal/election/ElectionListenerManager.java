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

package com.dangdang.ddframe.job.lite.internal.election;

import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.server.ServerNode;
import com.dangdang.ddframe.job.lite.internal.server.ServerService;
import com.dangdang.ddframe.job.lite.internal.server.ServerStatus;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

/**
 * 主节点选举监听管理器.
 *
 * @author zhangliang
 */
public final class ElectionListenerManager extends AbstractListenerManager {

    private final String jobName;

    private final LeaderNode leaderNode;

    private final ServerNode serverNode;

    private final LeaderService leaderService;

    private final ServerService serverService;

    public ElectionListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        leaderNode = new LeaderNode(jobName);
        serverNode = new ServerNode(jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }

    @Override
    public void start() {
        addDataListener(new LeaderElectionJobListener());
        addDataListener(new LeaderAbdicationJobListener());
    }

    /**
     * 主节点选举监听
     */
    class LeaderElectionJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isActiveElection(path, data) || isPassiveElection(path, eventType))) {
                // 进行主节点选举
                leaderService.electLeader();
            }
        }

        /**
         * 没主节点并且注册成功
         */
        private boolean isActiveElection(final String path, final String data) {
            return !leaderService.hasLeader() && isLocalServerEnabled(path, data);
        }

        private boolean isPassiveElection(final String path, final Type eventType) {
            // 主节点崩溃了 && 服务器开启了 && 有在线的服务器
            return isLeaderCrashed(path, eventType) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp());
        }

        /**
         * 主节点被删除了
         */
        private boolean isLeaderCrashed(final String path, final Type eventType) {
            // ${JOB_NAME}/leader/election/instance 并且 节点被删除了
            return leaderNode.isLeaderInstancePath(path) && Type.NODE_REMOVED == eventType;
        }

        /**
         * ${JOB_NAME}/servers/${IP} 注册成功并且是启用状态
         */
        private boolean isLocalServerEnabled(final String path, final String data) {
            // ${JOB_NAME}/servers/${IP} && 启用
            return serverNode.isLocalServerPath(path) && !ServerStatus.DISABLED.name().equals(data);
        }
    }

    /**
     * 主节点被关闭了
     */
    class LeaderAbdicationJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            // 是主节点 && 节点是禁用状态
            if (leaderService.isLeader() && isLocalServerDisabled(path, data)) {
                // 移除主节点
                leaderService.removeLeader();
            }
        }

        private boolean isLocalServerDisabled(final String path, final String data) {
            return serverNode.isLocalServerPath(path) && ServerStatus.DISABLED.name().equals(data);
        }
    }
}
