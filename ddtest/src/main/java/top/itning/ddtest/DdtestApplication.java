package top.itning.ddtest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@ImportResource(locations = {"classpath*:spring-job.xml"})
@SpringBootApplication
public class DdtestApplication {

    public static void main(String[] args) {
        SpringApplication.run(DdtestApplication.class, args);
    }

}
