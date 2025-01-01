package com.eclipseinfo.ibmmqdemo.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

/**
 * Author: Kidd
 * Date: 2024/12/27
 * Desc:
 */
@Service
@Slf4j
public class MessageProducer {

    @Value("${ibm.mq.queue}")
    private String DESTINATION;

    private final JmsTemplate jmsTemplate;

    @Autowired
    public MessageProducer(JmsTemplate jmsTemplate) {
        this.jmsTemplate = jmsTemplate;
    }

    public void sendOneMessage(String message) {
        jmsTemplate.convertAndSend(DESTINATION, message); // 替换为您的队列名称
        log.info("Sent message: " + message);
    }

    public void sendBatchMessage(int batchSize){
        for(int i=1; i<=batchSize; i++){
            jmsTemplate.convertAndSend(DESTINATION, Integer.toString(i));
        }
        log.info("Sent message batch size:" + batchSize);
    }
}
