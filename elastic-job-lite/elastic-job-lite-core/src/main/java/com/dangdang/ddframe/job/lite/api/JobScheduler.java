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

package com.dangdang.ddframe.job.lite.api;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.script.ScriptJob;
import com.dangdang.ddframe.job.event.JobEventBus;
import com.dangdang.ddframe.job.event.JobEventConfiguration;
import com.dangdang.ddframe.job.exception.JobConfigurationException;
import com.dangdang.ddframe.job.exception.JobSystemException;
import com.dangdang.ddframe.job.executor.JobFacade;
import com.dangdang.ddframe.job.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.api.strategy.JobInstance;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.guarantee.GuaranteeService;
import com.dangdang.ddframe.job.lite.internal.schedule.*;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.common.base.Optional;
import lombok.Getter;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 作业调度器.
 *
 * @author zhangliang
 * @author caohao
 */
public class JobScheduler {

    public static final String ELASTIC_JOB_DATA_MAP_KEY = "elasticJob";

    private static final String JOB_FACADE_DATA_MAP_KEY = "jobFacade";

    private final LiteJobConfiguration liteJobConfig;

    private final CoordinatorRegistryCenter regCenter;

    // TODO 为测试使用,测试用例不能反复new monitor service,以后需要把MonitorService重构为单例
    @Getter
    private final SchedulerFacade schedulerFacade;

    private final JobFacade jobFacade;

    public JobScheduler(final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration liteJobConfig, final ElasticJobListener... elasticJobListeners) {
        this(regCenter, liteJobConfig, new JobEventBus(), elasticJobListeners);
    }

    public JobScheduler(final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration liteJobConfig, final JobEventConfiguration jobEventConfig,
                        final ElasticJobListener... elasticJobListeners) {
        this(regCenter, liteJobConfig, new JobEventBus(jobEventConfig), elasticJobListeners);
    }

    private JobScheduler(final CoordinatorRegistryCenter regCenter, final LiteJobConfiguration liteJobConfig, final JobEventBus jobEventBus, final ElasticJobListener... elasticJobListeners) {
        // 添加JOB实例
        // new JobInstance() 作业实例主键为：192.168.0.110@-@8660 即 IP+JVM name
        JobRegistry.getInstance().addJobInstance(liteJobConfig.getJobName(), new JobInstance());
        this.liteJobConfig = liteJobConfig;
        this.regCenter = regCenter;
        List<ElasticJobListener> elasticJobListenerList = Arrays.asList(elasticJobListeners);
        // 对监听器的属性进行赋值
        setGuaranteeServiceForElasticJobListeners(regCenter, elasticJobListenerList);
        // 创建调度器
        schedulerFacade = new SchedulerFacade(regCenter, liteJobConfig.getJobName(), elasticJobListenerList);
        // 创建作业门面
        jobFacade = new LiteJobFacade(regCenter, liteJobConfig.getJobName(), Arrays.asList(elasticJobListeners), jobEventBus);
    }

    private void setGuaranteeServiceForElasticJobListeners(final CoordinatorRegistryCenter regCenter, final List<ElasticJobListener> elasticJobListeners) {
        GuaranteeService guaranteeService = new GuaranteeService(regCenter, liteJobConfig.getJobName());
        for (ElasticJobListener each : elasticJobListeners) {
            if (each instanceof AbstractDistributeOnceElasticJobListener) {
                ((AbstractDistributeOnceElasticJobListener) each).setGuaranteeService(guaranteeService);
            }
        }
    }

    /**
     * 初始化作业.
     */
    public void init() {
        // 获取生效的作业信息：1.检查作业 2.从ZK上获取作业信息并反序列化
        LiteJobConfiguration liteJobConfigFromRegCenter = schedulerFacade.updateJobConfiguration(liteJobConfig);
        // 设置该JOB的分片数量
        JobRegistry.getInstance().setCurrentShardingTotalCount(liteJobConfigFromRegCenter.getJobName(), liteJobConfigFromRegCenter.getTypeConfig().getCoreConfig().getShardingTotalCount());
        // 创建作业调度控制器
        JobScheduleController jobScheduleController = new JobScheduleController(
                // createScheduler创建调度器
                createScheduler(),
                // createJobDetail创建作业信息
                createJobDetail(liteJobConfigFromRegCenter.getTypeConfig().getJobClass()),
                liteJobConfigFromRegCenter.getJobName()
        );
        // 注册作业，添加作业调度控制器
        JobRegistry.getInstance().registerJob(liteJobConfigFromRegCenter.getJobName(), jobScheduleController, regCenter);
        // 注册作业启动信息
        schedulerFacade.registerStartUpInfo(!liteJobConfigFromRegCenter.isDisabled());
        // 调度作业 作业开始
        jobScheduleController.scheduleJob(liteJobConfigFromRegCenter.getTypeConfig().getCoreConfig().getCron());
    }

    /**
     * 创建作业信息
     */
    private JobDetail createJobDetail(final String jobClass) {
        JobDetail result = JobBuilder
                .newJob(LiteJob.class)
                .withIdentity(liteJobConfig.getJobName())
                .build();
        result.getJobDataMap().put(JOB_FACADE_DATA_MAP_KEY, jobFacade);
        // 从Spring进来的已经创建了
        Optional<ElasticJob> elasticJobInstance = createElasticJobInstance();
        if (elasticJobInstance.isPresent()) {
            result.getJobDataMap().put(ELASTIC_JOB_DATA_MAP_KEY, elasticJobInstance.get());
        } else if (!jobClass.equals(ScriptJob.class.getCanonicalName())) {
            try {
                result.getJobDataMap().put(ELASTIC_JOB_DATA_MAP_KEY, Class.forName(jobClass).newInstance());
            } catch (final ReflectiveOperationException ex) {
                throw new JobConfigurationException("Elastic-Job: Job class '%s' can not initialize.", jobClass);
            }
        }
        return result;
    }

    protected Optional<ElasticJob> createElasticJobInstance() {
        return Optional.absent();
    }

    private Scheduler createScheduler() {
        Scheduler result;
        try {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            factory.initialize(getBaseQuartzProperties());
            result = factory.getScheduler();
            // 作业触发监听器 目前是对错过执行的JOB进行监听
            result.getListenerManager().addTriggerListener(schedulerFacade.newJobTriggerListener());
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
        return result;
    }

    private Properties getBaseQuartzProperties() {
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", org.quartz.simpl.SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", "1");
        result.put("org.quartz.scheduler.instanceName", liteJobConfig.getJobName());
        result.put("org.quartz.jobStore.misfireThreshold", "1");
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }
}
