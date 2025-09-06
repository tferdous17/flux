package producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import commons.FluxTopic;
import metadata.InMemoryTopicMetadataRepository;
import server.internal.storage.Partition;

/**
 * Test the simplified RecordAccumulator without callbacks.
 */
public class RecordAccumulatorTest {

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
    void tearDown() throws IOException {
        if (accumulator != null) {
            accumulator.close();
        }

        // Clean up log files created by Partition objects
        Path logsDir = Paths.get("logs");
        if (Files.exists(logsDir)) {
            Files.walk(logsDir)
                    .filter(path -> path.toString().contains("test-topic"))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    @Test
    void testBatchCreationAndMetrics() throws IOException {
        // Create a test record with explicit partition to avoid metadata lookup
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", 0, System.currentTimeMillis(),
                "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Initial metrics should be zero
        RecordAccumulator.AccumulatorMetrics metrics = accumulator.getMetrics();
        assertEquals(0, metrics.batchesCreated);
        assertEquals(0, metrics.recordsAppended);
        assertEquals(0, metrics.batchesSent);
        assertEquals(0, metrics.batchesFailed);

        // Append a record
        accumulator.append(serializedRecord);

        // Check metrics after append
        metrics = accumulator.getMetrics();
        assertEquals(1, metrics.batchesCreated);
        assertEquals(1, metrics.recordsAppended);
        assertEquals(0, metrics.batchesSent);

        // Get the batch
        RecordBatch batch = accumulator.getCurrentBatch(0);
        assertNotNull(batch);

        // Flush and mark as sent
        var flushed = accumulator.flush();
        assertFalse(flushed.isEmpty());

        String batchId = batch.getBatchId();
        accumulator.markBatchSending(batchId);

        accumulator.markBatchSuccess(batchId);

        // Check final metrics
        metrics = accumulator.getMetrics();
        assertEquals(1, metrics.batchesCreated);
        assertEquals(1, metrics.recordsAppended);
        assertEquals(1, metrics.batchesSent);
        assertEquals(0, metrics.batchesFailed);
    }

    @Test
    void testBatchFailureMetrics() throws IOException {
        // Create a record with explicit partition to avoid metadata lookup
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", 0, System.currentTimeMillis(),
                "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);

        accumulator.append(serializedRecord);
        RecordBatch batch = accumulator.getCurrentBatch(0);
        String batchId = batch.getBatchId();

        var flushed = accumulator.flush();
        assertFalse(flushed.isEmpty());

        // Mark as failed
        accumulator.markBatchFailure(batchId, new RuntimeException("Test failure"));

        // Check metrics
        RecordAccumulator.AccumulatorMetrics metrics = accumulator.getMetrics();
        assertEquals(1, metrics.batchesCreated);
        assertEquals(1, metrics.recordsAppended);
        assertEquals(0, metrics.batchesSent);
        assertEquals(1, metrics.batchesFailed);
    }

    @Test
    void testMetricsToString() {
        RecordAccumulator.AccumulatorMetrics metrics = accumulator.getMetrics();
        String metricsStr = metrics.toString();

        assertTrue(metricsStr.contains("batches=(created=0, sent=0, failed=0)"));
        assertTrue(metricsStr.contains("records=0"));
        assertTrue(metricsStr.contains("inflight=0"));
    }
}