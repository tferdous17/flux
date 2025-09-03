package producer;

import commons.FluxTopic;
import commons.header.Header;
import commons.headers.Headers;
import metadata.InMemoryTopicMetadataRepository;
import org.junit.jupiter.api.Test;
import server.internal.storage.Partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecordAccumulatorTest {
    @Test
    public void appendTest() throws IOException {
        // Setup - Create the topic first
        String topicName = "test-topic";
        List<Partition> partitions = new ArrayList<>();
        partitions.add(new Partition("test-topic", 0)); // Add at least one partition
        FluxTopic fluxTopic = new FluxTopic(topicName, partitions, 1);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, fluxTopic);
        
        Headers headers = new Headers();
        headers.add(new Header("Kyoshi", "22".getBytes()));
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName,  // Use the created topic name
                0,
                System.currentTimeMillis(),
                "key",
                "22",
                headers
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Execute
        RecordAccumulator recordAccumulator = new RecordAccumulator(3);
        recordAccumulator.append(serializedData);
        recordAccumulator.printRecord();
    }
}
