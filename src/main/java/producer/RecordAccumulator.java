package producer;

import commons.utils.PartitionSelector;
import metadata.InMemoryTopicMetadataRepository;
import org.tinylog.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class RecordAccumulator {
    static private final int DEFAULT_BATCH_SIZE = 10_240; // 10 KB

    private final int batchSize;
    private Map<Integer, RecordBatch> partitionBatches; // Per-partition batches
    private final int numPartitions;

    public RecordAccumulator(int numPartitions) {
        this.batchSize = validateBatchSize(DEFAULT_BATCH_SIZE);
        this.partitionBatches = new HashMap<>();
        this.numPartitions = numPartitions;
    }

    public RecordAccumulator(int batchSize, int numPartitions) {
        this.batchSize = validateBatchSize(batchSize);
        this.partitionBatches = new HashMap<>();
        this.numPartitions = numPartitions;
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        return new RecordBatch(batchSize);
    }

    /**
     * Flush all current batches and return them for sending
     * @return Map of partition to RecordBatch for all non-empty batches
     */
    public Map<Integer, RecordBatch> flush() {
        if (partitionBatches.isEmpty()) {
            Logger.info("No batches to flush");
            return new HashMap<>();
        }
        
        Logger.info("Flushing " + partitionBatches.size() + " partition batches");
        
        // Create a copy of current batches to return
        Map<Integer, RecordBatch> batchesToFlush = new HashMap<>(partitionBatches);
        
        // Clear the current batches since they're being flushed
        partitionBatches.clear();
        
        return batchesToFlush;
    }

    /**
     * Convert RecordBatch map to list of IntermediaryRecords for gRPC sending
     * @param batchMap Map of partition to RecordBatch
     * @return List of IntermediaryRecord ready for gRPC
     */
    public List<IntermediaryRecord> convertBatchesToIntermediaryRecords(Map<Integer, RecordBatch> batchMap) {
        List<IntermediaryRecord> records = new ArrayList<>();
        
        for (Map.Entry<Integer, RecordBatch> entry : batchMap.entrySet()) {
            int partition = entry.getKey();
            RecordBatch batch = entry.getValue();
            
            // TODO: For now, we need to extract individual records from the batch
            // This is a temporary solution until we implement proper batch serialization
            ByteBuffer buffer = batch.getBatchBuffer();
            buffer.rewind(); // Reset position to read from beginning
            
            // For simplicity, we'll try to deserialize each record
            // Note: This is not ideal and should be improved in future iterations
            byte[] batchData = new byte[batch.getCurrBatchSizeInBytes()];
            buffer.get(batchData);
            
            // For now, treat the entire batch as one record per partition
            // TODO: Improve this to handle individual records within a batch
            ProducerRecord<String, String> tempRecord = ProducerRecordCodec.deserialize(
                batchData, String.class, String.class);
            
            records.add(new IntermediaryRecord(tempRecord.getTopic(), partition, batchData));
        }
        
        return records;
    }

    // TODO: There is a chance for refactoring here. Since we're deserializing a bit prematurely here, we can just
    //       move the logic into the FluxProducer class since we have access to the pre-serialized record there
    //       and basically "inline" the buffering. I.e., all the buffering mechanisms + partition routing can be
    //       moved to FluxProducer. Come back to this in a later ticket/PR.
    /**
     * Extract partition information from a serialized ProducerRecord
     */
    private int extractPartitionFromRecord(byte[] serializedRecord) {
        // Deserialize to get the ProducerRecord and extract partition info
        ProducerRecord<String, String> record = ProducerRecordCodec.deserialize(
                serializedRecord, String.class, String.class);

        return PartitionSelector.getPartitionNumberForRecord(
                InMemoryTopicMetadataRepository.getInstance(),
                record.getPartitionNumber(),
                record.getKey(),
                record.getTopic(),
                numPartitions
        );
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
        // Extract partition from the serialized record
        int partition = extractPartitionFromRecord(serializedRecord);
        RecordBatch currentBatch = partitionBatches.get(partition);
        
        int baseOffset = 0; // TODO: Should be determined by broker/partition
        
        try {
            if (currentBatch == null || !currentBatch.append(serializedRecord)) {
                if (currentBatch != null) { // Case 1B - batch is full
                    Logger.info("Batch for partition " + partition + " is full. Creating new batch.");
                    // Note: We don't auto-flush here, let the caller decide when to flush
                }
                Logger.info("Creating a new batch for partition " + partition + ".");
                currentBatch = createBatch(partition, baseOffset);
                partitionBatches.put(partition, currentBatch);
                
                if (!currentBatch.append(serializedRecord)) {
                    throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
                }
            }
            Logger.info("Record appended successfully to partition " + partition + ".");
        } catch (Exception e) {
            Logger.error("Failed to append record: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Get the current batch for a specific partition
     */
    public RecordBatch getCurrentBatch(int partition) {
        return partitionBatches.get(partition);
    }

    /**
     * Get the current batch for partition 0 (backward compatibility)
     */
    public RecordBatch getCurrentBatch() {
        return getCurrentBatch(0);
    }

    /**
     * Get all partition batches
     */
    public Map<Integer, RecordBatch> getPartitionBatches() {
        return new HashMap<>(partitionBatches); // Return copy to prevent external modification
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void printRecord() {
        Logger.info("Batch Size: " + getBatchSize());
        Logger.info("Partition Batches:");
        
        partitionBatches.forEach((partition, batch) -> {
            Logger.info("Partition " + partition + ":");
            batch.printBatchDetails();
        });
    }

    private int validateBatchSize(int batchSize) {
        final int MIN_BATCH_SIZE = 1; // Minimum size
        final int MAX_BATCH_SIZE = 1_048_576; // 1 MB

        if (batchSize < MIN_BATCH_SIZE || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "Batch size must be between " + MIN_BATCH_SIZE + "-" + MAX_BATCH_SIZE + " bytes."
            );
        }
        return batchSize;
    }
}
