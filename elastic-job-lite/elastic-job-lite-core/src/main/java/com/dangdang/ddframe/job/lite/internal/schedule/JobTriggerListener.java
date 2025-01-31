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

package com.dangdang.ddframe.job.lite.internal.schedule;

import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import lombok.RequiredArgsConstructor;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;

/**
 * 作业触发监听器.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class JobTriggerListener extends TriggerListenerSupport {

    private final ExecutionService executionService;

    private final ShardingService shardingService;

    @Override
    public String getName() {
        return "JobTriggerListener";
    }

    /**
     * 当Trigger错过被激发时执行,比如当前时间有很多触发器都需要执行，但是线程池中的有效线程都在工作，
     * 那么有的触发器就有可能超时，错过这一轮的触发。
     */
    @Override
    public void triggerMisfired(final Trigger trigger) {
        if (null != trigger.getPreviousFireTime()) {
            // 设置被错过执行的标记
            executionService.setMisfire(shardingService.getLocalShardingItems());
        }
    }
}
