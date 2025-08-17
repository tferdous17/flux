package broker;

import org.tinylog.Logger;
import producer.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages thread-safe write operations to partitions.
 * Ensures that writes to the same partition are sequential while allowing
 * parallel writes to different partitions.
 */
public class PartitionWriteManager {
    private final ConcurrentHashMap<Integer, ReentrantLock> partitionLocks;

    public PartitionWriteManager() {
        this.partitionLocks = new ConcurrentHashMap<>();
    }

    /**
     * Performs a thread-safe write to a partition.
     * Ensures atomic operation of: get offset -> update record header -> append to
     * partition
     * 
     * @param partition The target partition
     * @param record    The record data to write
     * @return The offset at which the record was written
     */
    public int writeToPartition(Partition partition, byte[] record) throws IOException {
        int partitionId = partition.getPartitionId();
        ReentrantLock lock = partitionLocks.computeIfAbsent(partitionId, _ -> new ReentrantLock(true));

        lock.lock();
        try {
            // Atomic operation: get offset, update record, and append
            int recordOffset = partition.getNextOffset();

            // Update record offset in header (first 4 bytes)
            ByteBuffer buffer = ByteBuffer.wrap(record);
            buffer.putInt(0, recordOffset);

            Logger.info("Writing to partition {} at offset {} | DATA = {}",
                    partitionId, recordOffset, java.util.Arrays.toString(buffer.array()));

            partition.appendSingleRecord(record, recordOffset);
            Logger.info("Successfully appended record to partition {} at offset {}", partitionId, recordOffset);

            return recordOffset;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Performs a thread-safe batch write to a partition.
     * Ensures that the entire batch is written atomically.
     * 
     * @param partition The target partition
     * @param batch     The record batch to write
     * @return The starting offset where the batch was written
     */
    public int writeRecordBatchToPartition(Partition partition, RecordBatch batch) throws IOException {
        int partitionId = partition.getPartitionId();
        ReentrantLock lock = partitionLocks.computeIfAbsent(partitionId, _ -> new ReentrantLock(true));

        lock.lock();
        try {
            Logger.info("Writing batch to partition {} with {} records",
                    partitionId, batch.getRecordCount());

            int startOffset = partition.appendRecordBatch(batch);

            Logger.info("Successfully appended batch to partition {} starting at offset {}",
                    partitionId, startOffset);

            return startOffset;
        } finally {
            lock.unlock();
        }
    }
}