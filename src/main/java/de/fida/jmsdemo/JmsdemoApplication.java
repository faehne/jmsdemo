package de.fida.jmsdemo;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.core.ProducerCallback;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import javax.jms.*;

@SpringBootApplication
public class JmsdemoApplication {

	@Bean
	public ConnectionFactory connectionFactory(){
		ActiveMQConnectionFactory activeMQConnectionFactory  = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616?jms.redeliveryPolicy.maximumRedeliveries=5");
		//activeMQConnectionFactory.setBrokerURL("vm://embedded??broker.persistent=false?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
		return  activeMQConnectionFactory;
	}
	@Bean
	public DefaultJmsListenerContainerFactory myFactory(){
		DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
		factory.setConnectionFactory(connectionFactory());
		//factory.setSessionTransacted(true);
		factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		return factory;
	}

	public static void main(String[] args) throws JMSException {
		ConfigurableApplicationContext ctx = SpringApplication.run(JmsdemoApplication.class, args);
		JmsTemplate jmsTemplate = ctx.getBean(JmsTemplate.class);
		JmsTransactionManager transactionManager = new JmsTransactionManager(jmsTemplate.getConnectionFactory());
		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
		jmsTemplate.setExplicitQosEnabled(true);
		jmsTemplate.setDeliveryPersistent(false);
		transactionTemplate.executeWithoutResult(transactionStatus -> {
			for(int i = 0; i < 50000;i++) {
				jmsTemplate.send("test2", session -> {
					TextMessage msg = session.createTextMessage("Hallo Welt");
					//msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
					return msg;
				});
			}
		});

//		throw new RuntimeException();

//		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
//		Connection conn = connectionFactory.createConnection();
//		Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//		Destination destination = session.createQueue("test1");
//		MessageProducer producer = session.createProducer(destination);
//
//		for(int i = 0; i < 100000;i++) {
//			TextMessage msg = session.createTextMessage("Hallo Welt");
//			msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//			producer.send(msg);
//		}
//		System.out.println("before commit");
//		//session.commit();
//		System.out.println("after commit");
//		session.close();
//		conn.close();
//		for(int i = 0; i < 100000;i++)
//			jmsTemplate.send("test2", session -> {
//				TextMessage msg = session.createTextMessage("Hallo Welt");
//				//msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//				return msg;
//			});
//		throw new RuntimeException();

	}

}
