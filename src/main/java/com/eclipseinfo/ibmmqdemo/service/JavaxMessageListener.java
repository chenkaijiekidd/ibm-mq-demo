package com.eclipseinfo.ibmmqdemo.service;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

@Component
@Slf4j
public class JavaxMessageListener implements MessageListener {

    private final List<Message> messageBatch = new ArrayList<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final int BATCH_SIZE = 10;

    @Autowired
    private Session session;

    @Override
    public void onMessage(Message message) {
        lock.lock();
        try {
            messageBatch.add(message);
            if (messageBatch.size() >= BATCH_SIZE) {
                log.info("============= Listener begin processing batch ============= ");
                processBatch();
                log.info("============= Listener begin processing batch ============= ");
            }
        } finally {
            lock.unlock();
        }
    }

    public void processBatch() {
        lock.lock();
        try {
            if (!messageBatch.isEmpty()) {
                log.info("Processing batch of size: " + messageBatch.size());
                // 处理消息批次
                for (Message message : messageBatch) {
                    //Thread.sleep(500);
                    // 在这里处理每个消息
                    //log.info("Processing message: " + message.getJMSMessageID());
                }
                // 提交会话
                session.commit();
                // 清空批次
                messageBatch.clear();
            }
        } catch (JMSException e) {
            e.printStackTrace();
            try {
                session.rollback();
            } catch (JMSException rollbackEx) {
                rollbackEx.printStackTrace();
            }
        }finally {
            lock.unlock();
        }
    }
}
