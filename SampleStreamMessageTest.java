import java.util.Hashtable;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

//
// A class that publishes and receives JMS StreamMessages. The SampleMessage
// contains
//     String _messageName;
//     long _messageNum;
//     String _regionName;
//     SampleComponent[] _components
// , where each SampleComponent has
//     int _key1;
//     int _key2;
//     double _value;
// The sending logic writes the common fields, the number of
// components and the individual components to javax.jms.StreamMessage and
// then sends it. The receiving logic reads the common fields, the number of
// components and the individual components from the received
// StreamMessage. The program fails with the following exception if
// the number of components in the array is greater than 40:
// Caused by: java.io.OptionalDataException
//        at java.base/java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1699)
//        at java.base/java.io.ObjectInputStream.readObject(ObjectInputStream.java:540)
//        at java.base/java.io.ObjectInputStream.readObject(ObjectInputStream.java:498)
//        at com.rabbitmq.jms.client.RMQMessage.readPrimitive(RMQMessage.java:1344)
//        at com.rabbitmq.jms.client.message.RMQStreamMessage.readPrimitiveType(RMQStreamMessage.java:94)
//        ... 10 more
public class SampleStreamMessageTest
    implements MessageListener
{
    private static final int NUM_OF_COMPONENTS = 50;

    private Context _context;
    private TopicConnectionFactory _tcf;
    private TopicConnection _tc;
    private final StringBuffer _lock = new StringBuffer();


    private TopicSession _publishSession;
    private TopicSession _subscribeSession;
    private StreamMessage _message;
    private Topic _topic;
    private MessageProducer _producer;
    private TopicSubscriber _topicSubscriber;

    // Initialize connection to the message broker
    private void init()
        throws JMSException, NamingException
    {
        _context = getInitialContext();
        _tcf = getTopicConnectionFactory();
        _tc = getTopicConnection();
        _topic = (Topic) _context.lookup("javax.jms.Topic.a.b");
        _tc.start();
    }

    // Return the initial context for RabbitMQ
    private Context getInitialContext()
    {
        String currentDir = System.getProperty("user.dir");
        System.out.println("Current directory is " + currentDir);
        String providerUrl = "file://" + currentDir + "/config/properties/include";
        System.out.println("providerUrl is " + providerUrl);

        Hashtable<Object,Object> props = new Hashtable<Object,Object>();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
        props.put(Context.PROVIDER_URL, providerUrl);

        Context context = null;
        try {
            context = new InitialContext(props);
        }
        catch (NamingException ex) {
            ex.printStackTrace();
            System.err.println("Unable to locate the object");
            System.exit(1);
        }

        return context;
    }

    // Lookup and return the TopicConnectionFactory for the message broker
    private TopicConnectionFactory getTopicConnectionFactory()
    {
        TopicConnectionFactory tcf = null;
        try {
            tcf = (TopicConnectionFactory) _context.lookup("ConnectionFactory");
        }
        catch (NamingException ex) {
            ex.printStackTrace();
            System.err.println("Unable to get connection factory");
            System.exit(1);
        }

        return tcf;
    }

    // Use the connection factory to create and return the topic connection
    public TopicConnection getTopicConnection()
    {
        TopicConnection connection = null;
        try {
            connection = _tcf.createTopicConnection();
        }
        catch (JMSException ex) {
            ex.printStackTrace();
            System.err.println("Unable to create topic connection");
            System.exit(1);
        }

        return connection;
    }

    // close the connection and all its derived objects
    protected synchronized void close()
    {
        System.out.println("Terminating connection...");
        try {
            _producer.close();
            _topicSubscriber.close();
            _publishSession.close();
            _subscribeSession.close();
            _tc.close();
            _context.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            System.err.println("Exception while terminating");
        }
    }

    // Send test StreamMessage to the messsage broker
    private void publish()
        throws JMSException, NamingException, InterruptedException
    {
        _publishSession = _tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        _message = _publishSession.createStreamMessage();
        _producer = _publishSession.createProducer(_topic);


        SampleMessage sm = new SampleMessage("TestSampleStreamMessage",
            1606832796513L, "US", NUM_OF_COMPONENTS);
        sm.convertToStreamMessage(_message);

        System.out.println("Sending stream message " + _message);
        _producer.send(_message);
        System.out.println("Sent stream message");
    }

    // Receive test StreamMessage from the messsage broker
    private void subscribe()
        throws JMSException, NamingException, InterruptedException
    {
        _subscribeSession = _tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        _topicSubscriber = _subscribeSession.createSubscriber(_topic, null, false);
        _topicSubscriber.setMessageListener(this);
        System.out.println("Receiving messages on topic " + _topic);
        synchronized (_lock) {
            _lock.wait();
        }
    }

    @Override
    public void onMessage(Message jmsMessage)
    {
        System.out.println("Received message " + jmsMessage);
        SampleMessage sm = new SampleMessage();
        try {
            sm.convertFromStreamMessage((StreamMessage) jmsMessage);
            System.out.println(sm);
        }
        catch (JMSException ex) {
            ex.printStackTrace();
            System.err.println("Exception while processing received message");
        }

        synchronized (_lock) {
            _lock.notifyAll();
        }
    }

    // Main program
    public static void main(String[] args)
        throws Exception
    {
        final SampleStreamMessageTest smt = new SampleStreamMessageTest();
        smt.init();

        Thread subscribeThread = new Thread() {
                public void run() {
                    try {
                        smt.subscribe();
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                        System.err.println("Exception in receiving thread");
                    }
                }
            };

        Thread publishThread = new Thread() {
                public void run() {
                    try {
                        smt.publish();
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                        System.err.println("Exception in publishing thread");
                    }
                }
            };


        subscribeThread.start();
        Thread.sleep(5000);
        publishThread.start();

        // Terminate properly when VM exits
        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run()
                {
                    smt.close();
                }
            });

        // Wait until interrupted manually
        try {
            Thread.sleep(60000);
        }
        catch (InterruptedException ex)
        {}
    }
}
