package de.fida.jmsdemo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

@Component
@Slf4j
public class TopicListener implements SessionAwareMessageListener<TextMessage> {

    @JmsListener(destination = "test.topic", containerFactory = "topicListenerFactory")
    public void onMessage(TextMessage message, Session session) throws JMSException {
        try {
            log.info("MSGID: {} MESSAGE: {}",message.getJMSMessageID(),message.getText());
        } catch(Exception ex) {
            log.error(ex.getMessage(),ex);
            throw(ex);
         }
    }
}