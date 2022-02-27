package de.fida.jmsdemo;

import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.stereotype.Component;

import javax.jms.*;

@Component
public class Listener implements SessionAwareMessageListener<TextMessage> {

    private long cnt = 0;
    private double sum = 0;

    @JmsListener(destination = "test", containerFactory = "myFactory")
    public void onMessage(TextMessage message, Session session) throws JMSException {
            cnt++;
            if(cnt > 1) {
                sum = sum + (System.currentTimeMillis() - message.getJMSTimestamp());
            }
            if(cnt % 1000 == 0) {
                System.out.println("In-Cnt: " + cnt + "... avg-msg-time (ms): " + (sum / 1000));
                sum = 0;
            }
            if(message.getJMSReplyTo() != null) {
                Message response = session.createTextMessage("Response to ID:" + message.getJMSCorrelationID());
                response.setJMSCorrelationID(message.getJMSCorrelationID());
                // Response
                MessageProducer producer = session.createProducer(message.getJMSReplyTo());
                producer.send(message.getJMSReplyTo(), response);
            }
            // Zum Testen: Rollback auslösen - geht nicht bei Autoack
            //throw new RuntimeException("--------------------ROLLBACK--------------------------");
    }
}