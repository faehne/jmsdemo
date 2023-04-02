package de.fida.jmsdemo;

import lombok.Cleanup;
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
@SuppressWarnings({"PMD.AvoidCatchingGenericException","PMD.UseUtilityClass"})
public class JmsdemoApplication {

    private final static String TEMPSESS = "tempsess";
    private final static String TEMPAUTO = "tempauto";
    private final static String JMSSESS = "jmssess";
    private final static String JMSAUTO = "jmsauto";
    private final static String JMSAUTOEXT = "jmsautoext";
    public static final String HALLO_WELT = "Hallo Welt";
    public static final String SENDING_COMPLETE = "<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>";

    public static void main(final String[] args) {
        @Cleanup final ConfigurableApplicationContext ctx = SpringApplication.run(JmsdemoApplication.class, args);
        @Cleanup final Scanner input = new Scanner(System.in);
        final ConnectionFactory connectionFactory = ctx.getBean("factoryQueue",ConnectionFactory.class);
        final JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory);
        try {
            mainLoop(input, jmsTemplate, connectionFactory);
        } catch(Exception ex) {
            log.info("ABORT APP");
        }
    }

    private static void mainLoop(final Scanner input, final JmsTemplate jmsTemplate, final ConnectionFactory connectionFactory) throws JMSException {
        while (true) {
            final String inputLine = input.nextLine();
            final String command = inputLine.split(" ")[0];
            final int msgCnt = inputLine.split(" ").length > 1 ? Integer.parseInt(inputLine.split(" ")[1]) : 10_000;
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

    private static void jmsAutoCommitExtended(final ConnectionFactory connectionFactory, final int msgCnt) throws JMSException {
        //////////////// 4. AutoAck über plain jms und replyTo und schedule ///////////////////////////
        @Cleanup final Connection conn = connectionFactory.createConnection();
        conn.start();
        @Cleanup final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue("test.jmsAutoCommitExtended.queue");
        final Destination tmpDest = session.createTemporaryQueue();
        @Cleanup final MessageConsumer responseConsumer = session.createConsumer(tmpDest);
        responseConsumer.setMessageListener(new ReplyToMessageListener());
        @Cleanup final MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT

        for (int i = 0; i < msgCnt; i++) {
            final TextMessage msg = session.createTextMessage(HALLO_WELT);
            if (i % 1000 == 0) {
                msg.setJMSCorrelationID(String.valueOf(i));
                msg.setJMSReplyTo(tmpDest);
            } else {
                msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 10 * 1000);
            }
            producer.send(msg);
        }
        log.info(SENDING_COMPLETE);
        try { //Notwendig, wenn die Temp-dest. länger bleiben muss (zB. bei Async send)
            log.info("<<<<<<<<<<WAIT>>>>>>>>>>>>>>>>>>>>");
            Thread.sleep(3000);
            log.info("<<<<<<<<<<WAIT COMPLETE>>>>>>>>>>>>>>>>>>>>");
        } catch (InterruptedException e) { }
    }

    private static void jmsAutoCommit(final ConnectionFactory connectionFactory, final int msgCnt) throws JMSException {
        //////////////// 4. AutoAck über plain jms /////////////////////////////////////////////////////
        @Cleanup final Connection conn = connectionFactory.createConnection();
        conn.start();
        @Cleanup final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue("test.jmsAutoCommit.queue");
        @Cleanup final MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT
        for (int i = 0; i < msgCnt; i++) {
            final TextMessage msg = session.createTextMessage(HALLO_WELT);
            producer.send(msg);
        }
        log.info(SENDING_COMPLETE);
    }

    private static void jmsSessionCommit(final ConnectionFactory connectionFactory, final int msgCnt) throws JMSException {
        //////////////// 3. Mit lokaler Sessiontransanction  über plain jms ///////////////////////////
        @Cleanup final Connection conn = connectionFactory.createConnection();
        @Cleanup final Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        final Destination destination = session.createQueue("test.jmsSessionCommit.queue");
        @Cleanup final MessageProducer producer = session.createProducer(destination);
        for (int i = 0; i < msgCnt; i++) {
            final TextMessage msg = session.createTextMessage(HALLO_WELT);
            msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT); //Kein DLQ-Handling, wenn NON_PERSISTENT
            producer.send(msg);
        }
        log.info(SENDING_COMPLETE);
        session.commit();
    }

    private static void templateAutoCommit(final JmsTemplate jmsTemplate, final int msgCnt) {
        //////////////// 2. Mit AutoAck  über JmsTemplate und non-persistent ///////////////////////////
        jmsTemplate.setExplicitQosEnabled(true);
        jmsTemplate.setDeliveryPersistent(true); //Kein DLQ-Handling, wenn false
        for (int i = 0; i < msgCnt; i++) {
            jmsTemplate.send("test.templateAutoCommit.queue", session -> session.createTextMessage(HALLO_WELT));
        }
        log.info("<<<<<<<<<<SENDING COMPLETE>>>>>>>>>>>>>>>>>>>>");
    }

    private static void templateSessionCommit(final JmsTemplate jmsTemplate, final int msgCnt) {
        //////////////// 1. Mit lokaler Sessiontransanction  über JmsTemplate ///////////////////////////
        final JmsTransactionManager transManager = new JmsTransactionManager(jmsTemplate.getConnectionFactory());
        final TransactionTemplate transTemplate = new TransactionTemplate(transManager);
        jmsTemplate.setExplicitQosEnabled(true);
        jmsTemplate.setDeliveryPersistent(true); //Kein DLQ-Handling, wenn false
        transTemplate.executeWithoutResult(transactionStatus -> {
            for (int i = 0; i < msgCnt; i++) {
                jmsTemplate.send("test.templateSessionCommit.queue", session -> session.createTextMessage(HALLO_WELT));
            }
        });
        log.info(SENDING_COMPLETE);
    }
}