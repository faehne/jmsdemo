package de.fida.jmsdemo;

import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

@Component
public class Listener  implements MessageListener {

    private long cnt = 0;
    private double avg = 1000;
    private double sum = 0;

    @JmsListener(destination = "test2", containerFactory = "myFactory")
    public void onMessage(Message message) {
        try {
            cnt++;
            sum = sum + (System.currentTimeMillis() - message.getJMSTimestamp());
            avg = sum / cnt;
            if(cnt % 1000 == 0) {
                System.out.println("In-Cnt: " + cnt + "... avg-msg-time: " + avg);
            }
            //throw new RuntimeException("--------------------ROLLBACK--------------------------");
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }
}