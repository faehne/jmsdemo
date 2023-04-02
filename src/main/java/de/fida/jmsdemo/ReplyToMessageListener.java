package de.fida.jmsdemo;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

@Slf4j
@NoArgsConstructor
public class ReplyToMessageListener implements MessageListener {

    @Override
    public void onMessage(final Message message) {
        try {
            if(log.isInfoEnabled()) {
                log.info("---ResponseMessage---- " + ((TextMessage) message).getText());
            }
        } catch (JMSException e) {
            if(log.isErrorEnabled()) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
