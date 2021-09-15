package top.itning.ddtest;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.api.simple.SimpleJob;

import java.time.LocalDateTime;

/**
 * @author itning
 * @since 2021/9/15 15:35
 */
public class TestJob implements SimpleJob {
    @Override
    public void execute(ShardingContext shardingContext) {
        System.out.println(LocalDateTime.now() + " " + Thread.currentThread().getName() + " " + shardingContext.toString());
    }
}
