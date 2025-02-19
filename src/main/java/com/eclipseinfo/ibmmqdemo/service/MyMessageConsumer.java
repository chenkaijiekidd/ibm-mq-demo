package com.eclipseinfo.ibmmqdemo.service;


import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.jms.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.util.StopWatch;

import java.io.IOException;
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
public class MyMessageConsumer {

    @Autowired
    private JmsTemplate jmsTemplate;
    @Value("${ibm.mq.queue}")
    private String queueName;
    private static final int BATCH_SIZE = 3;


    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    private List<Message> messages = new ArrayList<>();


    @Qualifier("myConnectionFactory")
    @Autowired
    private ConnectionFactory cf;

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

    //@Scheduled(fixedRate = 5, timeUnit = TimeUnit.SECONDS) // 每 2 秒消费一次，您可以根据需要调整
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
                StopWatch watch = new StopWatch();
                watch.start();
                jmsTemplate.setReceiveTimeout(10); // 10毫秒
                Message message = jmsTemplate.receive(queueName);
                watch.stop();
                log.info("consume message time: " + watch.getTotalTimeMillis() + "ms");
                if (message != null) {
                    messageList.add(message);
                } else {
                    break; // 如果没有更多消息则退出
                }
            }

            // 处理接收到的批量消息
            //processMessages(messageList);

            // 手动提交事务
            tm.commit(status);
            log.info("message batch process completed =========================================");

        }catch (Exception e){

            tm.rollback(status);
            e.printStackTrace();

        }
    }

    @PostConstruct
    public void init() throws JMSException {

        connection = cf.createConnection();
        connection.start();

        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (consumer != null) consumer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    //@Scheduled(fixedRate = 1000) // 每秒执行一次
    public void consumeMessages() {
        try {
            Message message = consumer.receiveNoWait(); // 非阻塞接收
            if (message != null) {
                messages.add(message);
                System.out.println("Received message. Current batch size: " + messages.size() + "|" + messages);
            }

            // 检查是否需要处理消息
            if (shouldProcessMessages()) {
                processAndAcknowledgeMessages();
            }
        } catch (Exception e) {
            System.err.println("Error receiving or processing message: " + e.getMessage());
        }
    }

    private boolean shouldProcessMessages() {
        return messages.size() >= BATCH_SIZE;
    }

    private void processAndAcknowledgeMessages() {
        if (messages.isEmpty()) {
            return;
        }

        try {
            //processAndWriteMessages(messages);
            for (Message msg : messages) {
                msg.acknowledge();
            }
            System.out.println("Batch processed and acknowledged successfully. Messages count: " + messages.size());
        } catch (Exception e) {
            System.err.println("Error processing batch: " + e.getMessage());
            // 不确认消息，它们将被重新传递
        } finally {
            messages.clear();
            //lastProcessTime = System.currentTimeMillis();
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
