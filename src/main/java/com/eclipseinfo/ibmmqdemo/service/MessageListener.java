package com.eclipseinfo.ibmmqdemo.service;

import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Kidd
 * Date: 2025/1/17
 * Desc:
 */

@Slf4j
@Component
public class MessageListener {

    //@JmsListener(destination = "DEV.QUEUE.1", containerFactory = "jmsListenerContainerFactory")
    public void consumMessage(Message message) throws Exception {

        String msgStr = ((TextMessage) message).getText();

        if (msgStr.equals("8")){
            throw new RuntimeException("Process error: " + msgStr);
        } else {
            log.info("Processing message: " + msgStr);
            // 手动确认消息
            message.acknowledge();
        }
    }

    private List<Message> batchMessages = new ArrayList<>();

    private static final int BATCH_SIZE = 5;

    //@JmsListener(destination = "DEV.QUEUE.1", containerFactory = "jmsListenerContainerFactory")
    public void consumMessage(Message message, Session session) throws Exception {
        String msgStr = ((TextMessage) message).getText();
        log.info("Received message: " + msgStr);

        try {
            if (batchMessages.size() < BATCH_SIZE) {
                batchMessages.add(message);
                if (batchMessages.size() == BATCH_SIZE) {
                    processAndAcknowledgeBatch();
                }
            } else {
                // 如果已经达到批处理大小，先处理当前批次
                processAndAcknowledgeBatch();
                // 然后开始新的批次
                batchMessages.add(message);
            }
        } catch (Exception e) {
            log.error("Error processing messages", e);
            // 发生错误时，清空批次并重新开始
            batchMessages.clear();
            throw e; // 重新抛出异常，让Spring重试机制处理
        }
    }

    private void processAndAcknowledgeBatch() throws Exception {
        try {
            processMessages();
            // 全部处理成功后，确认整个批次
            for (Message msg : batchMessages) {
                msg.acknowledge();
            }
            log.info("Batch processed and acknowledged successfully");
        } finally {
            batchMessages.clear();
        }
    }

    private void processMessages() throws Exception {
        for (Message message : batchMessages) {
            TextMessage textMessage = (TextMessage) message;
            int messageValue = Integer.parseInt(textMessage.getText());
            if (messageValue >= 8) {
                throw new RuntimeException("Process error: " + textMessage.getText());
            } else {
                log.info("Batch processing message: " + textMessage.getText());
            }
        }
        log.info("================== Process end of this batch ==========================");
    }


}
