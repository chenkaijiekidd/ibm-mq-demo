package com.eclipseinfo.ibmmqdemo.config;


import com.eclipseinfo.ibmmqdemo.service.JavaxMessageListener;
import com.ibm.mq.jakarta.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Session;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class JmsConfig {

    @Value("${ibm.mq.queueManager}")
    private String queueManager;

    @Value("${ibm.mq.channel}")
    private String channel;

    @Value("${ibm.mq.host}")
    private String host;

    @Value("${ibm.mq.port}")
    private int port;

    @Value("${ibm.mq.user}")
    private String user;

    @Value("${ibm.mq.password}")
    private String password;

/*    @Bean
    public PlatformTransactionManager transactionManager(ConnectionFactory connectionFactory){
        return new JmsTransactionManager(connectionFactory);
    }*/

    @Bean(name = "myConnectionFactory")   
    public ConnectionFactory connectionFactory() throws Exception {
        MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
        factory.setQueueManager(queueManager);
        factory.setChannel(channel);
        factory.setTransportType(1); // 1 = TCP
        factory.setHostName(host);
        factory.setPort(port);

        // 设置用户和密码
        factory.setStringProperty(WMQConstants.USERID, user);
        factory.setStringProperty(WMQConstants.PASSWORD, password);



        return factory;
    }

//    @Bean
//    public JmsTemplate jmsTemplate(@Qualifier("myConnectionFactory") ConnectionFactory connectionFactory) {
//        JmsTemplate template = new JmsTemplate(connectionFactory);
//        template.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
//        return template;
//    }

/*    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(@Qualifier("myConnectionFactory") ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        //factory.setSessionAcknowledgeMode(1); //auto ack
        //factory.setTransactionManager(transactionManager);
        //factory.setTransactionManager(new JmsTransactionManager(connectionFactory));
        factory.setSessionTransacted(false);
        factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        *//*factory.setSessionTransacted(true);
        factory.setSessionAcknowledgeMode(Session.SESSION_TRANSACTED);*//*
        return factory;
    }*/

    @Bean
    public DefaultMessageListenerContainer messageListenerContainer(@Qualifier("myConnectionFactory")ConnectionFactory connectionFactory, JavaxMessageListener messageListener) {
        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName("DEV.QUEUE.1");
        container.setMessageListener(messageListener);
        container.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        container.setSessionTransacted(true);
        return container;
    }

    @Bean
    public Session session(@Qualifier("myConnectionFactory")ConnectionFactory connectionFactory) throws Exception {
        return connectionFactory.createConnection().createSession(true, Session.SESSION_TRANSACTED);
    }
}