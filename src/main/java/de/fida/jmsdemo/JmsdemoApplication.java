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
		activeMQConnectionFactory.setUseAsyncSend(false); //true --> wesentlich schneller aber auch nicht mehr save
		//Embedded Version starten
		//activeMQConnectionFactory.setBrokerURL("vm://embedded??broker.persistent=false?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
		return  activeMQConnectionFactory;
	}
	@Bean
	public DefaultJmsListenerContainerFactory myFactory(ConnectionFactory connectionFactory){
		DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
		factory.setConnectionFactory(connectionFactory);

		////////////////////////// 1. Consume per Transaction ////////////////////////
//		factory.setSessionTransacted(true);
//		factory.setSessionAcknowledgeMode(Session.SESSION_TRANSACTED);

		////////////////////////// 2. Consume per AutoAck - Nachricht wird nicht wiederholt; kein DLQ-Handling ////////////////////////
		factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		return factory;
	}

	public static void main(String[] args) throws JMSException {
		ConfigurableApplicationContext ctx = SpringApplication.run(JmsdemoApplication.class, args);

		//////////////// 1. Mit lokaler Sessiontransanction  über JmsTemplate ///////////////////////////
//		JmsTemplate jmsTemplate = ctx.getBean(JmsTemplate.class);
//		JmsTransactionManager transactionManager = new JmsTransactionManager(jmsTemplate.getConnectionFactory());
//		TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
//		jmsTemplate.setExplicitQosEnabled(true);
//		jmsTemplate.setDeliveryPersistent(true); //Kein DLQ-Handling, wenn false
//		transactionTemplate.executeWithoutResult(transactionStatus -> {
//			for(int i = 0; i < 10000;i++) {
//				jmsTemplate.send("test", session -> {
//					TextMessage msg = session.createTextMessage("Hallo Welt");
//					return msg;
//				});
//			}
//		});

		//////////////// 2. Mit AutoAck  über JmsTemplate und non-persistent ///////////////////////////
//		JmsTemplate jmsTemplate = ctx.getBean(JmsTemplate.class);
//
//		jmsTemplate.setExplicitQosEnabled(true);
//		jmsTemplate.setDeliveryPersistent(false); //Kein DLQ-Handling, wenn false
//		for(int i = 0; i < 50000;i++) {
//			jmsTemplate.send("test", session -> {
//				TextMessage msg = session.createTextMessage("Hallo Welt");
//				return msg;
//			});
//		}

		//////////////// 3. Mit lokaler Sessiontransanction  über plain jms ///////////////////////////
//		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
//		Connection conn = connectionFactory.createConnection();
//		Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
//		Destination destination = session.createQueue("test");
//		MessageProducer producer = session.createProducer(destination);
//
//		for(int i = 0; i < 50000;i++) {
//			TextMessage msg = session.createTextMessage("Hallo Welt");
//			msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT
//			producer.send(msg);
//		}
//		session.commit();
//		session.close();
//		conn.close();

		//////////////// 4. AutoAck über plain jms und replyTo ///////////////////////////
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		Connection conn = connectionFactory.createConnection();
		conn.start();
		Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue("test");
		Destination tmpDest = session.createTemporaryQueue();
		MessageConsumer responseConsumer = session.createConsumer(tmpDest);
		responseConsumer.setMessageListener(new ReplyToMessageListener());
		MessageProducer producer = session.createProducer(destination);

		for(int i = 0; i < 50000;i++) {
			TextMessage msg = session.createTextMessage("Hallo Welt");
			msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT
			if(i % 1000 == 0) {
				msg.setJMSCorrelationID(String.valueOf(i));
				msg.setJMSReplyTo(tmpDest);
			}
			producer.send(msg);
		}
		session.close();
		conn.close();

	}

}