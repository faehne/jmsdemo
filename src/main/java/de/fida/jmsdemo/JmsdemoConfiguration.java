package de.fida.jmsdemo;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.connection.SingleConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

@Configuration
public class JmsdemoConfiguration {

    @Autowired
    ApplicationArguments appArgs;

    @Bean
    public ConnectionFactory connectionFactoryQueue() {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        //activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616?jms.redeliveryPolicy.maximumRedeliveries=5");
        //activeMQConnectionFactory.setUseAsyncSend(false); //true --> wesentlich schneller aber auch nicht mehr save
        //Embedded Version starten
        //activeMQConnectionFactory.setBrokerURL("vm://embedded??broker.persistent=false?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
        return new CachingConnectionFactory(activeMQConnectionFactory);
    }

    @Bean
    public ConnectionFactory connectionFactoryTopic() {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();

        SingleConnectionFactory singleConnectionFactory = new SingleConnectionFactory(activeMQConnectionFactory);
        singleConnectionFactory.setClientId("client-" + appArgs.getOptionValues("clientid").get(0));
        return singleConnectionFactory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory queueListenerFactory(ConnectionFactory connectionFactoryQueue) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactoryQueue);

        ////////////////////////// 1. Consume per Transaction ////////////////////////
//		factory.setSessionTransacted(true);
//		factory.setSessionAcknowledgeMode(Session.SESSION_TRANSACTED);

        ////////////////////////// 2. Consume per AutoAck - Nachricht wird nicht wiederholt; kein DLQ-Handling ////////////////////////
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        return factory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory topicListenerFactory(ConnectionFactory connectionFactoryTopic) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactoryTopic);
        factory.setPubSubDomain(true);
        factory.setSubscriptionDurable(true);
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        return factory;
    }
}
