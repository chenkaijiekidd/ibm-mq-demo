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

    @JmsListener(destination = "DEV.QUEUE.1", containerFactory = "jmsListenerContainerFactory")
    public void consumMessage(Message message) throws Exception {

        String msgStr = ((TextMessage) message).getText();

        if (msgStr.equals("25")){
            throw new RuntimeException("Process error: " + msgStr);
        } else {
            log.info("Processing message: " + msgStr);
            // 手动确认消息
            message.acknowledge();
        }
    }

    private List<Message> batchMessages = new ArrayList<>();

    private static final int BATCH_SIZE = 10;

    //@JmsListener(destination = "DEV.QUEUE.1", containerFactory = "jmsListenerContainerFactory")
    public void consumMessage(Message message, Session session) throws Exception {

        String msgStr = ((TextMessage) message).getText();
        //log.info("Current acknowledge mode: " + session.getAcknowledgeMode());
        log.info("Processing message: " + msgStr);

        try {
            if (batchMessages.size() < BATCH_SIZE) {
                batchMessages.add(message);
            } else {
                processMessages();
                // 在成功处理所有消息后，确认整个批次
                for (Message msg : batchMessages) {
                    msg.acknowledge();
                }
                batchMessages.clear();
            }
        } catch (Exception e) {
            log.error("Error processing messages", e);
            // 在 CLIENT_ACKNOWLEDGE 模式下，如果没有调用 acknowledge()，
            // 消息会被自动重新传递，所以这里不需要额外的操作
            //batchMessages.clear(); // 清空批次，准备重新接收
        }
    }

    private void processMessages() throws Exception {
        for (Message message : batchMessages) {
            TextMessage textMessage = (TextMessage) message;
            if (Integer.parseInt(textMessage.getText()) >= 25) {
                //throw new RuntimeException("Process error: " + textMessage.getText());
                System.exit(1);
            } else {
                log.info("Processing message: " + textMessage.getText());
            }
        }
        log.info("================== Process end of this batch ==========================");

    }

}
