package com.eclipseinfo.ibmmqdemo.service;


import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Kidd
 * Date: 2024/12/27
 * Desc:
 */
@Service
@Slf4j
@EnableTransactionManagement
public class MessageConsumer {

    @Autowired
    private JmsTemplate jmsTemplate;
    @Value("${ibm.mq.queue}")
    private String queueName;
    private static final int BATCH_SIZE = 100;

    // 提供一个Bean，用于动态返回队列名称
    @Bean(name = "messageDestination")
    public String messageDestination() {
        return queueName;
    }

    private void processMessages(List<Message> messages) throws Exception {
        for (Message message : messages) {
            // 处理消息逻辑
            TextMessage textMessage = (TextMessage) message;
            log.info("Processing message: " + textMessage.getText());
            // TODO: 确保确认/处理是基于业务逻辑
            if( Integer.parseInt(textMessage.getText()) >= 50){
                System.exit(1);
                //throw new RuntimeException("Process error!");
            }
        }

        // 处理完成后，由于事务管理，消息会自动确认
    }

    @Scheduled(fixedRate = 1000) // 每 2 秒消费一次，您可以根据需要调整
    public void receiveBatchMessages(){
        // Create a transaction manager object that will be used to control commit/rollback of operations.
        JmsTransactionManager tm = new JmsTransactionManager();

        // Associate the connection factory with the transaction manager
        tm.setConnectionFactory(jmsTemplate.getConnectionFactory());

        // This starts a new transaction scope. "null" can be used to get a default transaction model
        TransactionStatus status = tm.getTransaction(null);

        List<Message> messageList = new ArrayList<>();
        try{
            // 在会话中接收批量消息
            for (int i = 0; i < BATCH_SIZE; i++) {
                Message message = jmsTemplate.receive(queueName);
                if (message != null) {
                    messageList.add(message);
                } else {
                    break; // 如果没有更多消息则退出
                }
            }

            // 处理接收到的批量消息
            processMessages(messageList);

            // 手动提交事务
            tm.commit(status);

        }catch (Exception e){

            tm.rollback(status);
            e.printStackTrace();

        }
    }

 /*   @Scheduled(fixedRate = 1000) // 每 2 秒消费一次，您可以根据需要调整
    //@Transactional
    public void receiveBatchMessages() {

        List<Message> messageList = new ArrayList<>();
        Session session = null;

        try {
            // 创建会话
            session = jmsTemplate.getConnectionFactory().createConnection().createSession(true, Session.CLIENT_ACKNOWLEDGE);


            // 在会话中接收批量消息
            for (int i = 0; i < BATCH_SIZE; i++) {
                Message message = jmsTemplate.receive(queueName);
                if (message != null) {
                    messageList.add(message);
                } else {
                    break; // 如果没有更多消息则退出
                }
            }

            // 处理接收到的批量消息
            processMessages(messageList);

            // 手动提交事务
            session.commit();

        } catch (Exception e) {
            // 若发生异常，则回滚事务
            if (session != null) {
                try {
                    session.rollback();
                } catch (Exception rollbackEx) {
                    rollbackEx.printStackTrace();
                }
            }
            e.printStackTrace(); // 记录异常信息
        } finally {
            // 关闭会话
            if (session != null) {
                try {
                    session.close();
                } catch (Exception closeEx) {
                    closeEx.printStackTrace();
                }
            }
        }
    }

  */

 /*   @Scheduled(fixedRate = 1000) // 每 2 秒消费一次，您可以根据需要调整
    @JmsListener(destination = "DEV.QUEUE.1", containerFactory = "jmsListenerContainerFactory")
    public void receiveMessages(List<Message> messages, Session session) {

        try {
            processMessages(messages);
            // 所有消息处理成功，提交事务
            session.commit();
        } catch (Exception e) {
            // 处理失败，所有消息回滚
            try {
                session.rollback(); // 回滚事务
            } catch (Exception rollbackEx) {
                rollbackEx.printStackTrace();
            }
            e.printStackTrace(); // 记录异常信息
        }
    }*/

    /*@JmsListener(destination = "DEV.QUEUE.1")
    public void receiveMessage(String message){
        log.info("Received message:" + message);
    }*/
}
