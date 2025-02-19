package com.eclipseinfo.ibmmqdemo.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Author: Kidd
 * Date: 2025/2/19
 * Desc:
 */

@Component
@Slf4j
public class JavaxMessageScheduler {

    @Autowired
    private JavaxMessageListener messageListener;

    @Scheduled(fixedDelay = 2000) // 每30秒执行一次
    public void scheduledBatchProcessing() {
        log.info("============= Scheduler begin processing batch ============= ");
        messageListener.processBatch();
        log.info("============= Scheduler end processing batch ============= ");
    }
}
