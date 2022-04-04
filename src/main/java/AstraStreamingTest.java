import org.apache.pulsar.client.api.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AstraStreamingTest {
	
	private static final String SERVICE_URL = System.getenv("ASTRA_STREAM_URL");
	private static final String YOUR_PULSAR_TOKEN = System.getenv("ASTRA_STREAM_TOKEN");
	
	public static void main(String[] args) throws Exception {
        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(
                    AuthenticationFactory.token(YOUR_PULSAR_TOKEN)
                )
                .build();

        // Create producer on a topic
        Producer<byte[]> producer = client.newProducer()
                .topic("persistent://gameserver75/default/worldupdates")
                .create();

        // Send a message to the topic
        producer.send("Hello World".getBytes());

        // sleep to let the message take
        Thread.sleep(5 * 1000);

        //Close the producer
        producer.close();
               
        // Create consumer on a topic with a subscription
        Consumer consumer = client.newConsumer()
                .topic("gameserver75/default/worldupdates")
                .subscriptionName("my-subscription2")
                .subscribe();
        
        boolean receivedMsg = false;
        // Loop until a message is received
        do {
            // Block for up to 1 second for a message
            Message msg = consumer.receive(1, TimeUnit.SECONDS);

            if(msg != null){
                System.out.printf("Message received: %s", new String(msg.getData()));

                // Acknowledge the message to remove it from the message backlog
                consumer.acknowledge(msg);

                receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the consumer
        consumer.close();
        
        // Close the client
        client.close();	}
}
