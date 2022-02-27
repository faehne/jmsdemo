package de.fida.jmsdemo;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import javax.jms.*;
import java.util.Scanner;

@SpringBootApplication
public class JmsdemoApplication {

	private final static String HELP = "help";
	private final static String TEMPSESS = "tempsess";
	private final static String TEMPAUTO = "tempauto";
	private final static String JMSSESS = "jmssess";
	private final static String JMSAUTO = "jmsauto";


	@Bean
	public ConnectionFactory connectionFactory(){
		ActiveMQConnectionFactory activeMQConnectionFactory  = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616?jms.redeliveryPolicy.maximumRedeliveries=5");
		activeMQConnectionFactory.setUseAsyncSend(false); //true --> wesentlich schneller aber auch nicht mehr save
		//Embedded Version starten
		//activeMQConnectionFactory.setBrokerURL("vm://embedded??broker.persistent=false?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
		return  new CachingConnectionFactory(activeMQConnectionFactory);
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

		Scanner input = new Scanner(System.in);
		JmsTemplate jmsTemplate = ctx.getBean(JmsTemplate.class);
		ConnectionFactory connectionFactory = ctx.getBean(ConnectionFactory.class);
		while(true) {
			String command = input.nextLine();
			switch(command) {
				case HELP:
					System.out.println("tempsess - Mit lokaler Sessiontransanction  über JmsTemplate");
					System.out.println("tempauto - Mit AutoAck  über JmsTemplate und non-persistent");
					System.out.println("jmssess - Mit lokaler Sessiontransanction  über plain jms");
					System.out.println("jmsauto - AutoAck über plain jms und replyTo");
					break;
				case TEMPSESS:
					//////////////// 1. Mit lokaler Sessiontransanction  über JmsTemplate ///////////////////////////
					JmsTransactionManager transactionManager = new JmsTransactionManager(jmsTemplate.getConnectionFactory());
					TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
					jmsTemplate.setExplicitQosEnabled(true);
					jmsTemplate.setDeliveryPersistent(true); //Kein DLQ-Handling, wenn false
					transactionTemplate.executeWithoutResult(transactionStatus -> {
						for (int i = 0; i < 10000; i++) {
							jmsTemplate.send("test", session -> session.createTextMessage("Hallo Welt"));
						}
					});
					break;
				case TEMPAUTO:
					//////////////// 2. Mit AutoAck  über JmsTemplate und non-persistent ///////////////////////////
					jmsTemplate.setExplicitQosEnabled(true);
					jmsTemplate.setDeliveryPersistent(false); //Kein DLQ-Handling, wenn false
					for(int i = 0; i < 10000;i++) {
						jmsTemplate.send("test", session -> session.createTextMessage("Hallo Welt"));
					}
					break;
				case JMSSESS:
					//////////////// 3. Mit lokaler Sessiontransanction  über plain jms ///////////////////////////
					Connection conn = connectionFactory.createConnection();
					Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
					Destination destination = session.createQueue("test");
					MessageProducer producer = session.createProducer(destination);

					for(int i = 0; i < 10000;i++) {
						TextMessage msg = session.createTextMessage("Hallo Welt");
						msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT
						producer.send(msg);
					}
					session.commit();
					session.close();
					conn.close();
					break;
				case JMSAUTO:
					//////////////// 4. AutoAck über plain jms und replyTo ///////////////////////////
					Connection conn4 = connectionFactory.createConnection();
					conn4.start();
					Session session4 = conn4.createSession(false, Session.AUTO_ACKNOWLEDGE);
					Destination destination4 = session4.createQueue("test");
					Destination tmpDest = session4.createTemporaryQueue();
					MessageConsumer responseConsumer = session4.createConsumer(tmpDest);
					responseConsumer.setMessageListener(new ReplyToMessageListener());
					MessageProducer producer4 = session4.createProducer(destination4);

					for(int i = 0; i < 10000;i++) {
						TextMessage msg = session4.createTextMessage("Hallo Welt");
						msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT
						if(i % 1000 == 0) {
							msg.setJMSCorrelationID(String.valueOf(i));
							msg.setJMSReplyTo(tmpDest);
						}
						producer4.send(msg);
					}
					try { //Notwendig, wenn die Temp-dest. länger bleiben muss (zB. bei Async send)
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					session4.close();
					conn4.close();
			}
			}

	}

}