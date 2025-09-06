package producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import commons.FluxTopic;
import metadata.InMemoryTopicMetadataRepository;
import server.internal.storage.Partition;

import static org.junit.jupiter.api.Assertions.*;

public class CallbackTest {

    private RecordAccumulator accumulator;

    @BeforeEach
    void setUp() throws IOException {
        // Reset and setup metadata repository
        InMemoryTopicMetadataRepository.reset();
        InMemoryTopicMetadataRepository repo = InMemoryTopicMetadataRepository.getInstance();
        
        // Create partitions for the test topic (these will create log files)
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            partitions.add(new Partition("test-topic", i));
        }
        
        // Create a test topic with 3 partitions
        FluxTopic testTopic = new FluxTopic("test-topic", partitions, 1);
        repo.addNewTopic("test-topic", testTopic);
        
        accumulator = new RecordAccumulator(1024, 3, 100, 30000, 0.9, 32 * 1024 * 1024L, commons.compression.CompressionType.NONE);
    }

    @AfterEach
    void tearDown() {
        if (accumulator != null) {
            accumulator.close();
        }
    }

    @Test
    void testCallbackExecution() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final RecordMetadata[] result = new RecordMetadata[1];
        final Exception[] error = new Exception[1];

        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                result[0] = metadata;
                error[0] = exception;
                latch.countDown();
            }
        };

        // Create a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Append with callback
        RecordAppendResult appendResult = accumulator.append(serializedData, callback);
        assertNotNull(appendResult.future);
        assertTrue(appendResult.newBatchCreated);

        // Get batch ID before flushing
        String batchId = accumulator.getCurrentBatch(2).getBatchId(); // partition 2 from the logs
        // Flush and mark success
        accumulator.flush();
        accumulator.markBatchSuccess(batchId, 100L);

        // Wait for callback
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback should be invoked");
        assertNotNull(result[0], "RecordMetadata should not be null");
        assertNull(error[0], "Exception should be null for success");
        assertEquals("test-topic", result[0].topic());
        assertEquals(100L, result[0].offset());
    }

    @Test
    void testFutureCompletion() throws Exception {
        // Create a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Append without callback
        RecordAppendResult appendResult = accumulator.append(serializedData, null);
        Future<RecordMetadata> future = appendResult.future;
        
        assertFalse(future.isDone());

        // Get batch ID before flushing
        String batchId = accumulator.getCurrentBatch(2).getBatchId(); // partition 2 from the logs
        // Flush and mark success
        accumulator.flush();
        accumulator.markBatchSuccess(batchId, 200L);

        // Future should complete
        assertTrue(future.isDone());
        RecordMetadata metadata = future.get();
        assertNotNull(metadata);
        assertEquals("test-topic", metadata.topic());
        assertEquals(200L, metadata.offset());
    }

    @Test
    void testCallbackFailure() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final RecordMetadata[] result = new RecordMetadata[1];
        final Exception[] error = new Exception[1];

        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                result[0] = metadata;
                error[0] = exception;
                latch.countDown();
            }
        };

        // Create a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Append with callback
        RecordAppendResult appendResult = accumulator.append(serializedData, callback);
        
        // Get batch ID before flushing
        String batchId = accumulator.getCurrentBatch(2).getBatchId(); // partition 2 from the logs
        // Flush and mark failure
        accumulator.flush();
        RuntimeException testException = new RuntimeException("Test failure");
        accumulator.markBatchFailure(batchId, testException);

        // Wait for callback
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback should be invoked");
        assertNull(result[0], "RecordMetadata should be null for failure");
        assertNotNull(error[0], "Exception should not be null for failure");
        assertEquals("Test failure", error[0].getMessage());
    }
}