import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;


public class JmsTest {

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    private Connection connection;
    private Queue queue;
    private Session session;
    private MessageProducer producer;

    private MessageConsumer consumer;
    @Before
    public void before() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory("vm://embedded-broker?create=false");
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue("test");
        producer = session.createProducer(queue);
        consumer = session.createConsumer(queue);
    }

    @After
    public void after() throws JMSException {
        producer.close();
        consumer.close();
        session.close();
        connection.close();
    }
    @Test
    public void test() throws Exception {
        TextMessage message = session.createTextMessage("testing");
        producer.send(message);
        message = (TextMessage) consumer.receive();
        Assert.assertEquals("testing", message.getText());

    }

    @Test
    public void testMulti() throws Exception {
        int cntExpected = 200;
        for (int i = 0; i < cntExpected; i++) {
            TextMessage message = session.createTextMessage("testing" + i);
            producer.send(message);
        }
        for (int i = 0; i < cntExpected; i++) {
            TextMessage message = (TextMessage) consumer.receive();
            Assert.assertEquals("testing" + i, message.getText());
        }
    }

}
