package de.fida.jmsdemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ScheduledMessage;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import javax.jms.*;
import java.util.Scanner;

@SpringBootApplication
@Slf4j
public class JmsdemoApplication {

    private final static String TEMPSESS = "tempsess";
    private final static String TEMPAUTO = "tempauto";
    private final static String JMSSESS = "jmssess";
    private final static String JMSAUTO = "jmsauto";
    private final static String JMSAUTOEXT = "jmsautoext";

    public static void main(String[] args) throws JMSException {
        ConfigurableApplicationContext ctx = SpringApplication.run(JmsdemoApplication.class, args);
        Scanner input = new Scanner(System.in);
        ConnectionFactory connectionFactory = ctx.getBean("connectionFactoryQueue",ConnectionFactory.class);
        JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory);
        try {
            mainLoop(input, jmsTemplate, connectionFactory);
        } catch(Exception ex) {
            log.info("ABORT APP");
        }
    }

    private static void mainLoop(Scanner input, JmsTemplate jmsTemplate, ConnectionFactory connectionFactory) throws JMSException {
        while (true) {
            String inputLine = input.nextLine();
            String command = inputLine.split(" ")[0];
            int msgCnt = inputLine.split(" ").length > 1 ? Integer.parseInt(inputLine.split(" ")[1]) : 10000;
            switch (command) {
                case TEMPSESS:
                    templateSessionCommit(jmsTemplate, msgCnt);
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
        Connection conn = connectionFactory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("test.jmsAutoCommitExtended.queue");
        Destination tmpDest = session.createTemporaryQueue();
        MessageConsumer responseConsumer = session.createConsumer(tmpDest);
        responseConsumer.setMessageListener(new ReplyToMessageListener());
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT

        for (int i = 0; i < msgCnt; i++) {
            TextMessage msg = session.createTextMessage("Hallo Welt");
            if (i % 1000 == 0) {
                msg.setJMSCorrelationID(String.valueOf(i));
                msg.setJMSReplyTo(tmpDest);
            } else {
                msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 10 * 1000);
            }
            producer.send(msg);
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
        try { //Notwendig, wenn die Temp-dest. länger bleiben muss (zB. bei Async send)
            log.info("<<<<<<<<<<WAIT>>>>>>>>>>>>>>>>>>>>");
            Thread.sleep(3000);
            log.info("<<<<<<<<<<WAIT COMPLETE>>>>>>>>>>>>>>>>>>>>");
        } catch (InterruptedException e) { }
        session.close();
        conn.close();
    }

    private static void jmsAutoCommit(ConnectionFactory connectionFactory, int msgCnt) throws JMSException {
        //////////////// 4. AutoAck über plain jms /////////////////////////////////////////////////////
        Connection conn = connectionFactory.createConnection();
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue("test.jmsAutoCommit.queue");
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT

        for (int i = 0; i < msgCnt; i++) {
            TextMessage msg = session.createTextMessage("Hallo Welt");
            producer.send(msg);
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
        session.close();
        conn.close();
    }

    private static void jmsSessionCommit(ConnectionFactory connectionFactory, int msgCnt) throws JMSException {
        //////////////// 3. Mit lokaler Sessiontransanction  über plain jms ///////////////////////////
        Connection conn = connectionFactory.createConnection();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        Destination destination = session.createQueue("test.jmsSessionCommit.queue");
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
            jmsTemplate.send("test.templateAutoCommit.queue", session -> session.createTextMessage("Hallo Welt"));
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
    }

    private static void templateSessionCommit(JmsTemplate jmsTemplate, int msgCnt) {
        //////////////// 1. Mit lokaler Sessiontransanction  über JmsTemplate ///////////////////////////
        JmsTransactionManager transactionManager = new JmsTransactionManager(jmsTemplate.getConnectionFactory());
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        jmsTemplate.setExplicitQosEnabled(true);
        jmsTemplate.setDeliveryPersistent(true); //Kein DLQ-Handling, wenn false
        transactionTemplate.executeWithoutResult(transactionStatus -> {
            for (int i = 0; i < msgCnt; i++) {
                jmsTemplate.send("test.templateSessionCommit.queue", session -> session.createTextMessage("Hallo Welt"));
            }
        });
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
    }
}