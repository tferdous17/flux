package producer;

import commons.utils.PartitionSelector;
import metadata.InMemoryTopicMetadataRepository;
import org.tinylog.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RecordAccumulator {
    private final ProducerConfig config;
    private volatile long totalBytesUsed; // Track memory usage across all batches
    private Map<TopicPartition, RecordBatch> partitionBatches; // Per topic-partition batches
    private final int numPartitions;

    public RecordAccumulator(int numPartitions) {
        this(new ProducerConfig(), numPartitions);
    }

    public RecordAccumulator(int batchSize, int numPartitions) {
        this(new ProducerConfig(batchSize, 100, 33554432, true), numPartitions);
    }

    public RecordAccumulator(int batchSize, int maxBufferSize, int numPartitions) {
        this(new ProducerConfig(batchSize, 100, maxBufferSize, true), numPartitions);
    }
    
    public RecordAccumulator(ProducerConfig config, int numPartitions) {
        this.config = config;
        this.totalBytesUsed = 0;
        this.partitionBatches = new ConcurrentHashMap<>();
        this.numPartitions = numPartitions;
        validateBatchSize(config.getBatchSize());
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        return new RecordBatch(config.getBatchSize());
    }

    public boolean flush() {
        Logger.info("Flushing the batch to the broker (Stubbed out)");
        return true;
    }

    // TODO: There is a chance for refactoring here. Since we're deserializing a bit prematurely here, we can just
    //       move the logic into the FluxProducer class since we have access to the pre-serialized record there
    //       and basically "inline" the buffering. I.e., all the buffering mechanisms + partition routing can be
    //       moved to FluxProducer. Come back to this in a later ticket/PR.
    /**
     * Extract topic and partition information from a serialized ProducerRecord
     */
    private TopicPartition extractTopicPartitionFromRecord(byte[] serializedRecord) {
        // Deserialize to get the ProducerRecord and extract topic/partition info
        ProducerRecord<String, String> record = ProducerRecordCodec.deserialize(
                serializedRecord, String.class, String.class);

        int partition = PartitionSelector.getPartitionNumberForRecord(
                InMemoryTopicMetadataRepository.getInstance(),
                record.getPartitionNumber(),
                record.getKey(),
                record.getTopic(),
                numPartitions
        );
        
        return new TopicPartition(record.getTopic(), partition);
    }

    /*
    1. Check for both cases:
        1A) First-time batch exists -> currentBatch == null
        1B) Full batches -> !currentBatch.append(record)
    2. If we are NON-first-time batch, and it's full... we should flush and create a new batch since we flushed the old ones.
    3. Therefore, in both cases 1A and 1B we still need to call createBatch()... call createBatch() once only for both cases.
    4. If after logic, we still have a case where the batch is full... investigate further, return failure for now.
    */
    public void append(byte[] serializedRecord) throws IOException {
        // Check memory limits before proceeding
        if (totalBytesUsed + serializedRecord.length > config.getMaxBufferSize()) {
            throw new IllegalStateException(
                "Cannot append record: would exceed maximum buffer size of " + config.getMaxBufferSize() + " bytes. " +
                "Current usage: " + totalBytesUsed + " bytes, Record size: " + serializedRecord.length + " bytes."
            );
        }

        // Extract topic and partition from the serialized record
        TopicPartition topicPartition = extractTopicPartitionFromRecord(serializedRecord);
        RecordBatch currentBatch = partitionBatches.get(topicPartition);
        
        int baseOffset = 0; // TODO: Should be determined by broker/partition
        
        try {
            if (currentBatch == null || !currentBatch.append(serializedRecord)) {
                if (currentBatch != null) { // Case 1B - batch is full
                    Logger.info("Batch for {} is full. Flushing current batch.", topicPartition);
                    flush(); // TODO: Missing implementation
                }
                Logger.info("Creating a new batch for {}.", topicPartition);
                currentBatch = createBatch(topicPartition.getPartition(), baseOffset);
                partitionBatches.put(topicPartition, currentBatch);
                
                if (!currentBatch.append(serializedRecord)) {
                    throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
                }
            }
            
            // Update total bytes used
            totalBytesUsed += serializedRecord.length;
            Logger.info("Record appended successfully to {}. Total bytes used: {}", topicPartition, totalBytesUsed);
        } catch (Exception e) {
            Logger.error("Failed to append record: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Get the current batch for a specific topic-partition
     */
    public RecordBatch getCurrentBatch(String topic, int partition) {
        return partitionBatches.get(new TopicPartition(topic, partition));
    }

    /**
     * Get the current batch for partition 0 (backward compatibility)
     */
    public RecordBatch getCurrentBatch() {
        // Find first batch with partition 0
        for (Map.Entry<TopicPartition, RecordBatch> entry : partitionBatches.entrySet()) {
            if (entry.getKey().getPartition() == 0) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Get all partition batches
     */
    public Map<TopicPartition, RecordBatch> getPartitionBatches() {
        return new ConcurrentHashMap<>(partitionBatches); // Return copy to prevent external modification
    }

    public int getBatchSize() {
        return config.getBatchSize();
    }

    public long getTotalBytesUsed() {
        return totalBytesUsed;
    }

    public int getMaxBufferSize() {
        return config.getMaxBufferSize();
    }

    /**
     * Get list of topic-partitions with ready batches
     * A batch is ready if it's full OR has exceeded linger.ms
     * @return List of TopicPartition with ready batches
     */
    public List<TopicPartition> ready() {
        List<TopicPartition> readyPartitions = new ArrayList<>();
        long now = System.currentTimeMillis();
        
        for (Map.Entry<TopicPartition, RecordBatch> entry : partitionBatches.entrySet()) {
            RecordBatch batch = entry.getValue();
            if (batch != null) {
                boolean isFull = batch.isFull();
                boolean hasTimedOut = (now - batch.getCreationTime()) >= config.getLingerMs();
                
                if (isFull || hasTimedOut) {
                    readyPartitions.add(entry.getKey());
                    if (isFull) {
                        Logger.info("{} batch is ready - batch is full", entry.getKey());
                    } else {
                        Logger.info("{} batch is ready - exceeded linger.ms ({}ms)", 
                                   entry.getKey(), config.getLingerMs());
                    }
                }
            }
        }
        
        return readyPartitions;
    }
    
    /**
     * Drain ready batches from the accumulator
     * @param readyPartitions List of TopicPartition to drain
     * @return Map of drained batches by TopicPartition
     */
    public synchronized Map<TopicPartition, RecordBatch> drain(List<TopicPartition> readyPartitions) throws IOException {
        Map<TopicPartition, RecordBatch> drainedBatches = new ConcurrentHashMap<>();
        
        for (TopicPartition topicPartition : readyPartitions) {
            RecordBatch batch = partitionBatches.remove(topicPartition);
            if (batch != null) {
                // Compress if enabled
                if (config.isCompressionEnabled() && batch.getCurrBatchSizeInBytes() > 0) {
                    batch.compress();
                }
                
                // Update memory tracking
                decreaseMemoryUsage(batch);
                
                drainedBatches.put(topicPartition, batch);
                Logger.info("Drained batch from {} - size: {} bytes, compressed: {}",
                           topicPartition, batch.getDataSize(), batch.isCompressed());
            }
        }
        
        return drainedBatches;
    }
    
    /**
     * Helper method to decrease memory tracking when a batch is removed
     */
    private void decreaseMemoryUsage(RecordBatch batch) {
        totalBytesUsed -= batch.getCurrBatchSizeInBytes();
    }

    public void printRecord() {
        Logger.info("Batch Size: " + getBatchSize());
        Logger.info("Total Memory Used: " + totalBytesUsed + " / " + config.getMaxBufferSize() + " bytes");
        Logger.info("Topic-Partition Batches:");
        
        partitionBatches.forEach((topicPartition, batch) -> {
            Logger.info(topicPartition + ":");
            batch.printBatchDetails();
        });
    }

    private void validateBatchSize(int batchSize) {
        final int MIN_BATCH_SIZE = 1; // Minimum size
        final int MAX_BATCH_SIZE = 1_048_576; // 1 MB

        if (batchSize < MIN_BATCH_SIZE || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "Batch size must be between " + MIN_BATCH_SIZE + "-" + MAX_BATCH_SIZE + " bytes."
            );
        }
    }
    
    public ProducerConfig getConfig() {
        return config;
    }
}
