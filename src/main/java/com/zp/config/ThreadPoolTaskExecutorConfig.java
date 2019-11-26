package com.zp.config;

import lombok.Data;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@Data
public class ThreadPoolTaskExecutorConfig {

    private int corePoolSize = 5;

    private int maxPoolSize = 10;

    private int keepAliveSeconds = 200;

    private int queueCapacity = 100;

    private RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.AbortPolicy();

    private boolean waitForTasksToCompleteOnShutdown = false;

    private int awaitTerminationSeconds = 0;

    private String threadNamePrefix = "scheduled-";

    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix(threadNamePrefix);
        taskScheduler.setRejectedExecutionHandler(rejectedExecutionHandler);
        taskScheduler.setPoolSize(maxPoolSize);
        taskScheduler.setWaitForTasksToCompleteOnShutdown(waitForTasksToCompleteOnShutdown);
        taskScheduler.setAwaitTerminationSeconds(awaitTerminationSeconds);
        return taskScheduler;
    }


}
