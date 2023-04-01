package de.fida.jmsdemo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.stereotype.Component;

import javax.jms.*;

@Component
@Slf4j
public class Listener implements SessionAwareMessageListener<TextMessage> {

    private long cnt = 0;
    private double sum = System.currentTimeMillis();

    @Autowired
    ApplicationArguments args;

    //@JmsListener(destination = "test?consumer.exclusive=true", containerFactory = "myFactory")
    @JmsListener(destination = "test.*.queue", containerFactory = "queueListenerFactory", concurrency = "1")
    public void onMessage(TextMessage message, Session session) throws JMSException {
        try {
            cnt++;
            if (cnt % 1000 == 0) {
                log.info("In-Cnt: " + cnt + "... avg-msg-time (ms): " + ((System.currentTimeMillis() - sum) / 1000));
                sum = System.currentTimeMillis();
            }
            if(args.getOptionValues("printmsg") != null && args.getOptionValues("printmsg").get(0).equals("all")) {
                log.info("MSGID: {} MESSAGE: {}",message.getJMSMessageID(),message.getText());
            }

            if (message.getJMSReplyTo() != null) {
                log.info("---Message to response---- " + message.getText());
                Message response = session.createTextMessage("Response to ID:" + message.getJMSCorrelationID());
                response.setJMSCorrelationID(message.getJMSCorrelationID());
                // Response
                MessageProducer producer = session.createProducer(message.getJMSReplyTo());
                producer.send(message.getJMSReplyTo(), response);
            }
            // Zum Testen: Rollback auslösen - geht nicht bei Autoack
            //throw new RuntimeException("--------------------ROLLBACK--------------------------");
        } catch(Exception ex) {
            log.error(ex.getMessage(),ex);
            throw(ex);
         }
    }
}