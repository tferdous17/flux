package broker;

import grpc.BrokerServer;
import org.junit.jupiter.api.Test;
import producer.RecordBatch;
import server.internal.Broker;
import proto.Topic;

import java.io.IOException;
import java.util.List;

public class BrokerTest {

    @Test
    public void produceMessagesTest() throws IOException {
        Broker broker = new Broker();
        broker.setIsActiveController(true); // Enable topic creation
        
        // First create a topic
        Topic testTopic = Topic.newBuilder()
                .setTopicName("broker-test-topic")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
        broker.createTopics(List.of(testTopic));
        
        RecordBatch batch = new RecordBatch();

        // append fake data
        batch.append(new byte[]{1, 3, 2, 4, 9, 12, 34, 123, 93});
        batch.append(new byte[]{45, 4, 85, 5, 9, 12, 34, 123, 93});
        batch.append(new byte[]{14, 6, 72, 1, 121, 31, 34, 123, 93});
        batch.append(new byte[]{90, 3, 2, 0, 102, 12, 34, 123, 93});

        broker.produceMessages("broker-test-topic", batch);
    }

    @Test
    public void brokerShutdownTest() throws IOException {
        Broker broker = new Broker("broker-1", "localhost", 8080);
        BrokerServer server = new BrokerServer(broker);
        server.start(8080);
        broker.triggerManualBrokerShutdown();
    }
}
