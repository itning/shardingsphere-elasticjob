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

package com.dangdang.ddframe.job.lite.api.strategy.impl;

import com.dangdang.ddframe.job.lite.api.strategy.JobInstance;
import com.dangdang.ddframe.job.lite.api.strategy.JobShardingStrategy;

import java.util.*;

/**
 * 基于平均分配算法的分片策略.
 *
 * <p>
 * 如果分片不能整除, 则不能整除的多余分片将依次追加到序号小的服务器.
 * 如:
 * 1. 如果有3台服务器, 分成9片, 则每台服务器分到的分片是: 1=[0,1,2], 2=[3,4,5], 3=[6,7,8].
 * 2. 如果有3台服务器, 分成8片, 则每台服务器分到的分片是: 1=[0,1,6], 2=[2,3,7], 3=[4,5].
 * 3. 如果有3台服务器, 分成10片, 则每台服务器分到的分片是: 1=[0,1,2,9], 2=[3,4,5], 3=[6,7,8].
 * </p>
 *
 * @author zhangliang
 */
public final class AverageAllocationJobShardingStrategy implements JobShardingStrategy {

    @Override
    public Map<JobInstance, List<Integer>> sharding(final List<JobInstance> jobInstances, final String jobName, final int shardingTotalCount) {
        if (jobInstances.isEmpty()) {
            return Collections.emptyMap();
        }
        // TODO ITNING: 2021/9/15 重写了
        System.out.println("重新分片：");
        for (JobInstance jobInstance : jobInstances) {
            System.out.println(jobInstance.getJobInstanceId());
        }
        Map<JobInstance, List<Integer>> result = shardingAliquot(jobInstances, shardingTotalCount);
        addAliquant(jobInstances, shardingTotalCount, result);
        System.out.println("分片结果：");
        for (Map.Entry<JobInstance, List<Integer>> entry : result.entrySet()) {
            System.out.println(entry.getKey().getJobInstanceId() + "  " + entry.getValue());
        }
        return result;
    }

    private Map<JobInstance, List<Integer>> shardingAliquot(final List<JobInstance> shardingUnits, final int shardingTotalCount) {
        Map<JobInstance, List<Integer>> result = new LinkedHashMap<>(shardingTotalCount, 1);
        int itemCountPerSharding = shardingTotalCount / shardingUnits.size();
        int count = 0;
        for (JobInstance each : shardingUnits) {
            List<Integer> shardingItems = new ArrayList<>(itemCountPerSharding + 1);
            for (int i = count * itemCountPerSharding; i < (count + 1) * itemCountPerSharding; i++) {
                shardingItems.add(i);
            }
            result.put(each, shardingItems);
            count++;
        }
        return result;
    }

    private void addAliquant(final List<JobInstance> shardingUnits, final int shardingTotalCount, final Map<JobInstance, List<Integer>> shardingResults) {
        int aliquant = shardingTotalCount % shardingUnits.size();
        int count = 0;
        for (Map.Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) {
            if (count < aliquant) {
                entry.getValue().add(shardingTotalCount / shardingUnits.size() * shardingUnits.size() + count);
            }
            count++;
        }
    }
}
