import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;


public class JmsTest {

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    @Test
    public void test() throws Exception {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://embedded-broker?create=false");
        final Queue queue;
        final Connection connection = connectionFactory.createConnection();
        connection.start();
        final Session session = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue("test");
        final MessageProducer producer = session.createProducer(queue);
        TextMessage message = session.createTextMessage("testing");
        producer.send(message);
        MessageConsumer consumer = session.createConsumer(queue);
        message = (TextMessage) consumer.receive();
        Assert.assertEquals("testing", message.getText());

    }

}
