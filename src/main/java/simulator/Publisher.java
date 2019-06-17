package simulator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class Publisher {
    private XMLMessageProducer prod;
    private JCSMPSession session;
    private Topic topic;

    public Publisher() throws JCSMPException {
        // Create a JCSMP Session
        Properties props = getProperties();

        // String uri = "tcp://vmr-mr3e5sq7dacxp.messaging.solace.cloud:20480";
        // String username = "solace-cloud-client";
        // String password = "cge4fi7lj67ms6mnn2b4pe76g2";
        // String vpn = "msgvpn-8ksiwsp0mtv";

        String uri = props.getProperty("broker.uri");
        String username = props.getProperty("broker.username");
        String password = props.getProperty("broker.password");
        String vpn = props.getProperty("broker.vpn");
        



        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, uri);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, username); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  vpn); // message-vpn
        properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
        session =  JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();
        topic = JCSMPFactory.onlyInstance().createTopic("payment/tx");

        /** Anonymous inner-class for handling publishing events */
        prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                System.out.println("Producer received response for msg: " + messageID);
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                System.out.printf("Producer received error for msg: %s@%s - %s%n",
                        messageID,timestamp,e);
            }
        });

    }

    public void sendMessage() throws JCSMPException {
        // Publish-only session is now hooked up and running!

        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        final String text = "Hello world!";
        msg.setText(text);
        System.out.printf("Connected. About to send message '%s' to topic '%s'...%n",text,topic.getName());
        prod.send(msg,topic);
        System.out.println("Message sent. Exiting.");

    }

    public void sendMessage(byte[] bytes) throws JCSMPException {
        // Publish-only session is now hooked up and running!

        BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        msg.setData(bytes);
        System.out.printf("Connected. About to send message to topic '%s'...%n",topic.getName());
        prod.send(msg,topic);
        System.out.println("Message sent. Exiting.");

    }


    @Override
    protected void finalize() throws Throwable {
        session.closeSession();
        super.finalize();
    }

    public  Properties getProperties(){
        try (InputStream input = App.class.getClassLoader().getResourceAsStream("application.properties")) {
            Properties props = new Properties();
    
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return null;
            }

            //load a properties file from class path, inside static method
            props.load(input);
    
            //get the property value and print it out
            System.out.println(props.getProperty("broker.username"));
            return props;
    
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }


}