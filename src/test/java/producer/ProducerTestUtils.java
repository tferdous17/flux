package producer;

import commons.FluxTopic;
import commons.header.Header;
import commons.headers.Headers;
import metadata.InMemoryTopicMetadataRepository;
import server.internal.storage.Partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ProducerTestUtils {
    
    public static void setupTestTopic(String topicName, int numPartitions) throws IOException {
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new Partition(topicName, i));
        }
        FluxTopic fluxTopic = new FluxTopic(topicName, partitions, 1);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, fluxTopic);
    }
    
    public static ProducerRecord<String, String> createTestRecord(
            String topicName, int partition, String key, String value) {
        return new ProducerRecord<>(topicName, partition, System.currentTimeMillis(), key, value, null);
    }
    
    public static byte[] serializeRecord(ProducerRecord<String, String> record) throws IOException {
        return ProducerRecordCodec.serialize(record, String.class, String.class);
    }
    
    public static Headers createTestHeaders(String key, String value) {
        Headers headers = new Headers();
        headers.add(new Header(key, value.getBytes()));
        return headers;
    }
    
    public static class CallbackTracker implements BatchCallback {
        private final AtomicInteger createdCount = new AtomicInteger(0);
        private final AtomicInteger readyCount = new AtomicInteger(0);
        private final AtomicInteger sendingCount = new AtomicInteger(0);
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final AtomicReference<Object> lastResult = new AtomicReference<>();
        private final AtomicReference<Throwable> lastException = new AtomicReference<>();
        
        @Override
        public void onBatchCreated(BatchInfo batchInfo) {
            createdCount.incrementAndGet();
        }
        
        @Override
        public void onBatchReady(BatchInfo batchInfo) {
            readyCount.incrementAndGet();
        }
        
        @Override
        public void onBatchSending(BatchInfo batchInfo) {
            sendingCount.incrementAndGet();
        }
        
        @Override
        public void onBatchSuccess(BatchInfo batchInfo, Object result) {
            successCount.incrementAndGet();
            lastResult.set(result);
        }
        
        @Override
        public void onBatchFailure(BatchInfo batchInfo, Throwable exception) {
            failureCount.incrementAndGet();
            lastException.set(exception);
        }
        
        public int getCreatedCount() { return createdCount.get(); }
        public int getReadyCount() { return readyCount.get(); }
        public int getSendingCount() { return sendingCount.get(); }
        public int getSuccessCount() { return successCount.get(); }
        public int getFailureCount() { return failureCount.get(); }
        public Object getLastResult() { return lastResult.get(); }
        public Throwable getLastException() { return lastException.get(); }
        
        public void reset() {
            createdCount.set(0);
            readyCount.set(0);
            sendingCount.set(0);
            successCount.set(0);
            failureCount.set(0);
            lastResult.set(null);
            lastException.set(null);
        }
    }
}