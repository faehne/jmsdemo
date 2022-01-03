package de.fida.jmsdemo;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import javax.jms.*;

@SpringBootApplication
public class JmsdemoApplication {

	@Bean
	public ConnectionFactory connectionFactory(){
		ActiveMQConnectionFactory activeMQConnectionFactory  = new ActiveMQConnectionFactory();
		//activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
		activeMQConnectionFactory.setBrokerURL("vm://embedded??broker.persistent=false?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
		return  activeMQConnectionFactory;
	}
	@Bean
	public DefaultJmsListenerContainerFactory myFactory(){
		DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
		factory.setConnectionFactory(connectionFactory());
		factory.setSessionTransacted(true);
		factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		return factory;
	}

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(JmsdemoApplication.class, args);
		JmsTemplate jmsTemplate = ctx.getBean(JmsTemplate.class);
		jmsTemplate.setExplicitQosEnabled(true);
		jmsTemplate.setDeliveryPersistent(false);
		for(int i = 0; i < 50000;i++)
			jmsTemplate.send("test1", new MessageCreator() {
				@Override
				public Message createMessage(Session session) throws JMSException {
					TextMessage msg = session.createTextMessage("Hallo Welt");
					//msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
					return msg;
				}
			});

	}

}
