package de.fida.jmsdemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import javax.jms.*;
import java.util.Scanner;

@SpringBootApplication
@Slf4j
public class JmsdemoApplication {

    private final static String HELP = "help";
    private final static String TEMPSESS = "tempsess";
    private final static String TEMPAUTO = "tempauto";
    private final static String JMSSESS = "jmssess";
    private final static String JMSAUTO = "jmsauto";
    private final static String JMSAUTOEXT = "jmsautoext";

    @Bean
    public ConnectionFactory connectionFactory() {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
        //activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616?jms.redeliveryPolicy.maximumRedeliveries=5");
        //activeMQConnectionFactory.setUseAsyncSend(false); //true --> wesentlich schneller aber auch nicht mehr save
        //Embedded Version starten
        //activeMQConnectionFactory.setBrokerURL("vm://embedded??broker.persistent=false?jms.redeliveryPolicy.maximumRedeliveries=5&jms.useAsyncSend=true");
        return new CachingConnectionFactory(activeMQConnectionFactory);
    }

    @Bean
    public DefaultJmsListenerContainerFactory myFactory(ConnectionFactory connectionFactory) {
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
        try {
            mainLoop(input, jmsTemplate, connectionFactory);
        } catch(Exception ex) {
            log.info("ABORT APP");
        }
    }

    private static void mainLoop(Scanner input, JmsTemplate jmsTemplate, ConnectionFactory connectionFactory) throws JMSException {
        while (true) {
            String inputLine = getInputLine(input);
            String command = inputLine.split(" ")[0];
            int msgCnt = inputLine.split(" ").length > 1 ? Integer.parseInt(inputLine.split(" ")[1]) : 10000;
            switch (command) {
                case TEMPSESS:
                    templateSession(jmsTemplate, msgCnt);
                    break;
                case TEMPAUTO:
                    templateAutoCommit(jmsTemplate, msgCnt);
                    break;
                case JMSSESS:
                    jmsSessionCommit(connectionFactory, msgCnt);
                    break;
                case JMSAUTO:
                    jmsAutoCommit(connectionFactory, msgCnt);
                    break;
                case JMSAUTOEXT:
                    jmsAutoCommitExtended(connectionFactory, msgCnt);
                    break;
                default:
                    log.info("tempsess - Mit lokaler Sessiontransanction  über JmsTemplate");
                    log.info("tempauto - Mit AutoAck  über JmsTemplate und non-persistent");
                    log.info("jmssess - Mit lokaler Sessiontransanction  über plain jms");
                    log.info("jmsauto - AutoAck über plain jms");
                    log.info("jmsautoext - AutoAck über plain jms und replyTo und schedule");
                    break;
            }
        }
    }

    private static void jmsAutoCommitExtended(ConnectionFactory connectionFactory, int msgCnt) throws JMSException {
        //////////////// 4. AutoAck über plain jms und replyTo und schedule ///////////////////////////
        Connection conn5 = connectionFactory.createConnection();
        conn5.start();
        Session session5 = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination5 = session5.createQueue("test");
        Destination tmpDest = session5.createTemporaryQueue();
        MessageConsumer responseConsumer = session5.createConsumer(tmpDest);
        responseConsumer.setMessageListener(new ReplyToMessageListener());
        MessageProducer producer5 = session5.createProducer(destination5);
        producer5.setDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT

        for (int i = 0; i < msgCnt; i++) {
            TextMessage msg = session5.createTextMessage("Hallo Welt");
            if (i % 1000 == 0) {
                msg.setJMSCorrelationID(String.valueOf(i));
                msg.setJMSReplyTo(tmpDest);
            } else {
                msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 10 * 1000);
            }
            producer5.send(msg);
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
        try { //Notwendig, wenn die Temp-dest. länger bleiben muss (zB. bei Async send)
            log.info("<<<<<<<<<<WAIT>>>>>>>>>>>>>>>>>>>>");
            Thread.sleep(3000);
            log.info("<<<<<<<<<<WAIT COMPLETE>>>>>>>>>>>>>>>>>>>>");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        session5.close();
        conn5.close();
    }

    private static void jmsAutoCommit(ConnectionFactory connectionFactory, int msgCnt) throws JMSException {
        //////////////// 4. AutoAck über plain jms /////////////////////////////////////////////////////
        Connection conn4 = connectionFactory.createConnection();
        conn4.start();
        Session session4 = conn4.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination4 = session4.createQueue("test");
        MessageProducer producer4 = session4.createProducer(destination4);
        producer4.setDeliveryMode(DeliveryMode.NON_PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT

        for (int i = 0; i < msgCnt; i++) {
            TextMessage msg = session4.createTextMessage("Hallo Welt");
            producer4.send(msg);
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
        session4.close();
        conn4.close();
    }

    private static void jmsSessionCommit(ConnectionFactory connectionFactory, int msgCnt) throws JMSException {
        //////////////// 3. Mit lokaler Sessiontransanction  über plain jms ///////////////////////////
        Connection conn = connectionFactory.createConnection();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue("test");
        MessageProducer producer = session.createProducer(destination);

        for (int i = 0; i < msgCnt; i++) {
            TextMessage msg = session.createTextMessage("Hallo Welt");
            msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT
            producer.send(msg);
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
        session.commit();
        session.close();
        conn.close();
    }

    private static void templateAutoCommit(JmsTemplate jmsTemplate, int msgCnt) {
        //////////////// 2. Mit AutoAck  über JmsTemplate und non-persistent ///////////////////////////
        jmsTemplate.setExplicitQosEnabled(true);
        jmsTemplate.setDeliveryPersistent(true); //Kein DLQ-Handling, wenn false
        for (int i = 0; i < msgCnt; i++) {
            jmsTemplate.send("test", session -> session.createTextMessage("Hallo Welt"));
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
    }

    private static void templateSession(JmsTemplate jmsTemplate, int msgCnt) {
        //////////////// 1. Mit lokaler Sessiontransanction  über JmsTemplate ///////////////////////////
        JmsTransactionManager transactionManager = new JmsTransactionManager(jmsTemplate.getConnectionFactory());
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        jmsTemplate.setExplicitQosEnabled(true);
        jmsTemplate.setDeliveryPersistent(true); //Kein DLQ-Handling, wenn false
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            for (int i = 0; i < msgCnt; i++) {
                jmsTemplate.send("test", session -> session.createTextMessage("Hallo Welt"));
            }
        });
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
    }

    private static String getInputLine(Scanner input) {
        String inputLine = null;
        inputLine = input.nextLine();
        log.info("ABORT");
        return inputLine;
    }

}