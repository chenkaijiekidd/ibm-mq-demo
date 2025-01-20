package com.eclipseinfo.ibmmqdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IbmMqDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(IbmMqDemoApplication.class, args);
    }

}
