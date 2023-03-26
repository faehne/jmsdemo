package de.fida.jmsdemo;

import lombok.extern.slf4j.Slf4j;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

@Slf4j
public class ReplyToMessageListener implements MessageListener {
    @Override
    public void onMessage(Message message) {
        try {
            log.info("---ResponseMessage---- " + ((TextMessage) message).getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
