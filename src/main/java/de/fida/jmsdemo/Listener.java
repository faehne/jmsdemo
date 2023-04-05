package de.fida.jmsdemo;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

@Component
@Slf4j
@SuppressWarnings("PMD.AvoidCatchingGenericException")
public class Listener implements SessionAwareMessageListener<TextMessage> {

    private long cnt;
    private double sum = System.currentTimeMillis();

    private final ApplicationArguments appArgs;

    public Listener(final ApplicationArguments appArgs) {
        this.appArgs = appArgs;
    }

    //@JmsListener(destination = "test?consumer.exclusive=true", containerFactory = "myFactory")
    @JmsListener(destination = "test.*.queue", containerFactory = "queueListenerFactory", concurrency = "1")
    @Override
    public void onMessage(final TextMessage message, final Session session) throws JMSException {
        try {
            cnt++;
            printCntAndDuration();
            printSingleMessage(message);

            checkAndHandleReplyTo(message, session);
            // Zum Testen: Rollback auslösen - geht nicht bei Autoack
            //throw new RuntimeException("--------------------ROLLBACK--------------------------");
        } catch(Exception ex) {
            if(log.isErrorEnabled()) {
                log.error(ex.getMessage(), ex);
            }
            throw ex;
         }
    }

    private void checkAndHandleReplyTo(final TextMessage message, final Session session) throws JMSException {
        if (message.getJMSReplyTo() != null) {
            if(log.isInfoEnabled()) {
                log.info("---Message to response---- {}", message.getText());
            }
            final TextMessage response =
                    session.createTextMessage("Response to ID:" + message.getJMSCorrelationID());
            response.setJMSCorrelationID(message.getJMSCorrelationID());
            // Response
            @Cleanup final MessageProducer producer = session.createProducer(message.getJMSReplyTo());
            producer.send(message.getJMSReplyTo(), response);
        }
    }

    private void printSingleMessage(final TextMessage message) throws JMSException {
        if(appArgs.getOptionValues("printmsg") != null &&
                appArgs.getOptionValues("printmsg").get(0).equals("all")) {
            if(log.isInfoEnabled()) {
                log.info("MSGID: {} MESSAGE: {}", message.getJMSMessageID(), message.getText());
            }
        }
    }

    private void printCntAndDuration() {
        if (cnt % 1000 == 0) {
            log.info("In-Cnt: {}... avg-msg-time (ms): {}",cnt,(System.currentTimeMillis() - sum) / 1000);
            sum = System.currentTimeMillis();
        }
    }
}