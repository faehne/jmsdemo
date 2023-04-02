package de.fida.jmsdemo;

import org.apache.activemq.ActiveMQConnectionFactory;
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


    private final ApplicationArguments appArgs;

    public JmsdemoConfiguration(final ApplicationArguments appArgs) {
        this.appArgs = appArgs;
    }

    @Bean
    public ConnectionFactory factoryQueue() {
        final ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory();
        //activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616?jms.redeliveryPolicy.maximumRedeliveries=5");
        //activeMQConnectionFactory.setUseAsyncSend(false); //true --> wesentlich schneller aber auch nicht mehr save
        //Embedded Version starten
        //activeMQConnectionFactory.setBrokerURL("vm://embedded??broker.persistent=false?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
        return new CachingConnectionFactory(connFactory);
    }

    @Bean
    public ConnectionFactory factoryTopic() {
        final ActiveMQConnectionFactory connFactory = new ActiveMQConnectionFactory();

        final SingleConnectionFactory singleFactory = new SingleConnectionFactory(connFactory);
        singleFactory.setClientId("client-" + appArgs.getOptionValues("clientid").get(0));
        return singleFactory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory queueListenerFactory(final ConnectionFactory factoryQueue) {
        final DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(factoryQueue);

        ////////////////////////// 1. Consume per Transaction ////////////////////////
//		factory.setSessionTransacted(true);
//		factory.setSessionAcknowledgeMode(Session.SESSION_TRANSACTED);

        ////////////////////////// 2. Consume per AutoAck - Nachricht wird nicht wiederholt; kein DLQ-Handling ////////////////////////
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        return factory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory topicListenerFactory(final ConnectionFactory factoryTopic) {
        final DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(factoryTopic);
        factory.setPubSubDomain(true);
        factory.setSubscriptionDurable(true);
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        return factory;
    }
}
