package de.fida.jmsdemo;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;

@Component
@Slf4j
@NoArgsConstructor
public class TopicListener implements SessionAwareMessageListener<TextMessage> {

    @JmsListener(destination = "test.topic", containerFactory = "topicListenerFactory")
    @Override
    public void onMessage(final TextMessage message, final Session session) throws JMSException {
        try {
            if(log.isInfoEnabled()) {
                log.info("MSGID: {} MESSAGE: {}",message.getJMSMessageID(),message.getText());
            }
        } catch(JMSException ex) {
            if(log.isErrorEnabled()) {
                log.error(ex.getMessage(), ex);
            }
            throw ex;
         }
    }
}