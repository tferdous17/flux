package producer;

import commons.CompressionType;
import commons.utils.PartitionSelector;
import metadata.InMemoryTopicMetadataRepository;
import metadata.Metadata;
import metadata.snapshots.ClusterSnapshot;
import metadata.snapshots.PartitionMetadata;
import metadata.snapshots.TopicMetadata;
import org.tinylog.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordAccumulator {
    private final ProducerConfig config;
    private final BufferPool free; // BufferPool for memory management
    private Map<TopicPartition, Deque<RecordBatch>> partitionBatches; // Per topic-partition batch queues
    private final Map<TopicPartition, AtomicInteger> inFlightBatches; // Track in-flight batches per partition
    private final int numPartitions;
    private final Map<String, Integer> drainIndexPerBroker = new ConcurrentHashMap<>(); // Per-broker round-robin indexes

    public RecordAccumulator(int numPartitions) {
        this(new ProducerConfig(), numPartitions);
    }

    
    public RecordAccumulator(ProducerConfig config, int numPartitions) {
        this.config = config;
        this.free = new BufferPool(config.getBufferMemory(), config.getBatchSize());
        this.partitionBatches = new ConcurrentHashMap<>();
        this.inFlightBatches = new ConcurrentHashMap<>();
        this.numPartitions = numPartitions;
    }

    public RecordBatch createBatch(int partition, long baseOffset) throws InterruptedException {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        
        // Allocate buffer from BufferPool
        java.nio.ByteBuffer buffer = free.allocate(config.getBatchSize(), config.getMaxBlockMs());
        return new RecordBatch(buffer, config.getCompressionType());
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

        // Extract topic and partition from the serialized record
        TopicPartition topicPartition = extractTopicPartitionFromRecord(serializedRecord);
        
        // Get or create deque for this topic-partition
        Deque<RecordBatch> deque = partitionBatches.computeIfAbsent(
            topicPartition, k -> new ArrayDeque<>());
        
        int baseOffset = 0; // TODO: Should be determined by broker/partition
        
        try {
            // Try to append to the last batch in the deque (Kafka approach)
            RecordBatch lastBatch = deque.peekLast();
            if (lastBatch != null && lastBatch.append(serializedRecord)) {
                // Successfully appended to existing batch
                Logger.info("Record appended to existing batch for {}.", topicPartition);
                return;
            }
            
            // Need to create a new batch
            Logger.info("Creating a new batch for {}.", topicPartition);
            RecordBatch newBatch = createBatch(topicPartition.getPartition(), baseOffset);
            deque.addLast(newBatch);
            
            if (!newBatch.append(serializedRecord)) {
                // If record doesn't fit, we need to deallocate the buffer and throw exception
                free.deallocate(newBatch.getBuffer(), newBatch.getInitialCapacity());
                deque.removeLast(); // Remove the batch we just added
                throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
            }
            
            Logger.info("Record appended to new batch for {}.", topicPartition);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for buffer memory", e);
        } catch (IllegalStateException e) {
            Logger.error("Buffer allocation failed: " + e.getMessage());
            throw new IOException("Cannot allocate buffer memory: " + e.getMessage(), e);
        } catch (Exception e) {
            Logger.error("Failed to append record: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Get the current batch for a specific topic-partition
     */
    public RecordBatch getCurrentBatch(String topic, int partition) {
        Deque<RecordBatch> deque = partitionBatches.get(new TopicPartition(topic, partition));
        return deque != null ? deque.peekLast() : null;
    }


    /**
     * Get all partition batches (current/last batch for each partition)
     */
    public Map<TopicPartition, RecordBatch> getPartitionBatches() {
        Map<TopicPartition, RecordBatch> result = new ConcurrentHashMap<>();
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : partitionBatches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            if (deque != null && !deque.isEmpty()) {
                result.put(entry.getKey(), deque.peekLast()); // Return the current (last) batch
            }
        }
        return result;
    }

    public int getBatchSize() {
        return config.getBatchSize();
    }

    public long getBufferMemory() {
        return config.getBufferMemory();
    }

    public long getTotalBytesUsed() {
        return free.totalMemory() - free.availableMemory();
    }


    /**
     * Expire old batches that have exceeded the delivery timeout
     * @param now Current time in milliseconds
     */
    private void expireOldBatches(long now) {
        long deliveryTimeoutMs = config.getDeliveryTimeoutMs();
        
        // Iterate through all partition batches
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : partitionBatches.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();
            
            if (deque != null && !deque.isEmpty()) {
                // Check batches from oldest to newest
                Iterator<RecordBatch> iterator = deque.iterator();
                while (iterator.hasNext()) {
                    RecordBatch batch = iterator.next();
                    long age = now - batch.getCreationTime();
                    
                    // Check if batch has expired
                    if (age > deliveryTimeoutMs) {
                        // Remove the expired batch
                        iterator.remove();
                        
                        // Free the buffer back to the pool
                        free.deallocate(batch.getBuffer(), batch.getInitialCapacity());
                        
                        // Log the expiration
                        Logger.warn("Batch expired for {} after {}ms (delivery.timeout.ms={}ms). " +
                                   "Records: {}, Size: {} bytes", 
                                   topicPartition, age, deliveryTimeoutMs, 
                                   batch.getRecordCount(), batch.getCurrBatchSizeInBytes());
                    } else {
                        // Batches are ordered by age, so if this one hasn't expired, 
                        // newer ones won't have either
                        break;
                    }
                }
            }
        }
    }
    
    /**
     * Get the broker address for a given topic-partition
     * @param topicPartition The topic-partition to lookup
     * @return The broker address (e.g., "localhost:50051") or null if not found
     */
    public String getBrokerForPartition(TopicPartition topicPartition) {
        ClusterSnapshot snapshot = Metadata.getInstance().getClusterMetadataSnapshot().get();
        
        // Get topic metadata
        TopicMetadata topicMetadata = snapshot.topics().get(topicPartition.getTopic());
        if (topicMetadata == null) {
            Logger.warn("Topic {} not found in metadata", topicPartition.getTopic());
            return null;
        }
        
        // Get partition metadata
        PartitionMetadata partitionMetadata = topicMetadata.partitions().get(topicPartition.getPartition());
        if (partitionMetadata == null) {
            Logger.warn("Partition {} for topic {} not found in metadata", 
                       topicPartition.getPartition(), topicPartition.getTopic());
            return null;
        }
        
        // Return the broker ID (which is the broker address in our implementation)
        return partitionMetadata.brokerId();
    }
    
    /**
     * Get all topic-partitions assigned to a specific broker
     * @param brokerAddress The broker address to get partitions for
     * @return List of TopicPartition assigned to this broker
     */
    private List<TopicPartition> getPartitionsForBroker(String brokerAddress) {
        List<TopicPartition> brokerPartitions = new ArrayList<>();
        ClusterSnapshot snapshot = Metadata.getInstance().getClusterMetadataSnapshot().get();
        
        // Iterate through all topics and partitions
        for (Map.Entry<String, TopicMetadata> topicEntry : snapshot.topics().entrySet()) {
            String topic = topicEntry.getKey();
            TopicMetadata topicMetadata = topicEntry.getValue();
            
            for (Map.Entry<Integer, PartitionMetadata> partitionEntry : topicMetadata.partitions().entrySet()) {
                int partition = partitionEntry.getKey();
                PartitionMetadata partitionMetadata = partitionEntry.getValue();
                
                // Check if this partition is assigned to the broker
                if (brokerAddress.equals(partitionMetadata.brokerId())) {
                    brokerPartitions.add(new TopicPartition(topic, partition));
                }
            }
        }
        
        // Sort for consistent ordering
        brokerPartitions.sort((a, b) -> {
            int topicCompare = a.getTopic().compareTo(b.getTopic());
            if (topicCompare != 0) return topicCompare;
            return Integer.compare(a.getPartition(), b.getPartition());
        });
        
        return brokerPartitions;
    }
    
    /**
     * Get list of topic-partitions with ready batches
     * A batch is ready if it's full OR has exceeded linger.ms
     * Also checks in-flight limits to prevent overwhelming the broker
     * @return List of TopicPartition with ready batches
     */
    public List<TopicPartition> ready() {
        List<TopicPartition> readyPartitions = new ArrayList<>();
        long now = System.currentTimeMillis();
        
        // First, expire old batches before checking readiness
        expireOldBatches(now);
        
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : partitionBatches.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();
            
            if (deque != null && !deque.isEmpty()) {
                // Check if we've hit the in-flight limit for this partition
                int inFlightCount = getInFlightCount(topicPartition);
                if (inFlightCount >= config.getMaxInFlightRequests()) {
                    Logger.debug("{} has {} in-flight batches, skipping (max: {})", 
                               topicPartition, inFlightCount, config.getMaxInFlightRequests());
                    continue;
                }
                
                // Check the first (oldest) batch in the deque
                RecordBatch firstBatch = deque.peekFirst();
                if (firstBatch != null) {
                    boolean isFull = firstBatch.isFull();
                    boolean hasTimedOut = (now - firstBatch.getCreationTime()) >= config.getLingerMs();
                    
                    if (isFull || hasTimedOut) {
                        readyPartitions.add(topicPartition);
                        if (isFull) {
                            Logger.info("{} batch is ready - batch is full", topicPartition);
                        } else {
                            Logger.info("{} batch is ready - exceeded linger.ms ({}ms)", 
                                       topicPartition, config.getLingerMs());
                        }
                    }
                }
            }
        }
        
        return readyPartitions;
    }
    
    /**
     * Drain ready batches from the accumulator for a specific broker
     * Uses per-broker round-robin to ensure fairness across all partitions
     * @param brokerAddress The broker address to drain batches for
     * @param readyPartitions List of TopicPartition that have ready batches
     * @param maxSize Maximum size in bytes to drain
     * @return Map of drained batches by TopicPartition
     */
    public synchronized Map<TopicPartition, RecordBatch> drain(String brokerAddress, List<TopicPartition> readyPartitions, int maxSize) throws IOException {
        Map<TopicPartition, RecordBatch> drainedBatches = new ConcurrentHashMap<>();
        
        if (readyPartitions.isEmpty() || brokerAddress == null) {
            return drainedBatches;
        }
        
        // Get ALL partitions for this broker (not just ready ones) for stable round-robin
        List<TopicPartition> brokerPartitions = getPartitionsForBroker(brokerAddress);
        if (brokerPartitions.isEmpty()) {
            Logger.warn("No partitions found for broker {}", brokerAddress);
            return drainedBatches;
        }
        
        // Get or initialize the drain index for this broker
        int brokerDrainIndex = drainIndexPerBroker.computeIfAbsent(brokerAddress, k -> 0);
        
        // Track total size drained and last successfully drained index
        int totalSize = 0;
        int partitionCount = brokerPartitions.size();
        int current = brokerDrainIndex % partitionCount;
        int partitionsChecked = 0;
        int lastDrainedIndex = current;
        
        // Keep draining until we hit size limit or check all partitions
        while (partitionsChecked < partitionCount && totalSize < maxSize) {
            TopicPartition topicPartition = brokerPartitions.get(current);
            
            // Check if this partition is in the ready list
            if (readyPartitions.contains(topicPartition)) {
                Deque<RecordBatch> deque = partitionBatches.get(topicPartition);
                if (deque != null && !deque.isEmpty()) {
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        int batchSize = batch.estimatedSizeInBytes();
                        
                        // Check if adding this batch would exceed limit
                        // Always drain at least one batch even if it exceeds maxSize
                        if (totalSize + batchSize <= maxSize || drainedBatches.isEmpty()) {
                            // Poll and drain the batch
                            batch = deque.pollFirst();
                            processBatchForDraining(batch, topicPartition, brokerAddress, drainedBatches, deque);
                            totalSize += batchSize;
                            lastDrainedIndex = current;
                        } else {
                            // Would exceed size limit, stop draining
                            break;
                        }
                    }
                }
            }
            
            current = (current + 1) % partitionCount;
            partitionsChecked++;
        }
        
        // Update index to continue from where we left off
        // Move to the next partition after the last drained one
        drainIndexPerBroker.put(brokerAddress, (lastDrainedIndex + 1) % partitionCount);
        
        if (drainedBatches.isEmpty() && !readyPartitions.isEmpty()) {
            Logger.debug("No batches drained for broker {} despite {} ready partitions", 
                        brokerAddress, readyPartitions.size());
        } else if (!drainedBatches.isEmpty()) {
            Logger.debug("Drained {} batches ({} bytes) for broker {}", 
                        drainedBatches.size(), totalSize, brokerAddress);
        }
        
        return drainedBatches;
    }
    
    /**
     * Helper method to process a batch for draining
     */
    private void processBatchForDraining(RecordBatch batch, TopicPartition topicPartition, 
                                        String brokerAddress, Map<TopicPartition, RecordBatch> drainedBatches,
                                        Deque<RecordBatch> deque) throws IOException {
        // Compress the batch (will only compress if type != NONE)
        if (batch.getCurrBatchSizeInBytes() > 0) {
            batch.compress();
        }
        
        // Deallocate buffer back to pool
        free.deallocate(batch.getBuffer(), batch.getInitialCapacity());
        
        drainedBatches.put(topicPartition, batch);
        Logger.info("Drained batch from {} for broker {} - size: {} bytes, compressed: {}, remaining batches: {}",
                   topicPartition, brokerAddress, batch.getDataSize(), batch.isCompressed(), deque.size());
    }
    

    public void printRecord() {
        Logger.info("Batch Size: " + getBatchSize());
        Logger.info("Total Memory Used: " + getTotalBytesUsed() + " / " + free.totalMemory() + " bytes");
        Logger.info("Available Memory: " + free.availableMemory() + " bytes");
        Logger.info("Queued Threads: " + free.queued());
        Logger.info("Topic-Partition Batches:");
        
        partitionBatches.forEach((topicPartition, deque) -> {
            Logger.info(topicPartition + " (queued batches: " + deque.size() + "):");
            int batchNum = 0;
            for (RecordBatch batch : deque) {
                Logger.info("  Batch " + batchNum + ":");
                batch.printBatchDetails();
                batchNum++;
            }
        });
    }

    
    public ProducerConfig getConfig() {
        return config;
    }
    
    /**
     * Get BufferPool for testing and monitoring
     * @return BufferPool instance
     */
    public BufferPool getBufferPool() {
        return free;
    }
    
    /**
     * Increment the in-flight batch count for a topic-partition
     * @param topicPartition The topic-partition to increment
     */
    public void incrementInFlight(TopicPartition topicPartition) {
        inFlightBatches.computeIfAbsent(topicPartition, k -> new AtomicInteger(0)).incrementAndGet();
    }
    
    /**
     * Decrement the in-flight batch count for a topic-partition
     * @param topicPartition The topic-partition to decrement
     */
    public void decrementInFlight(TopicPartition topicPartition) {
        AtomicInteger count = inFlightBatches.get(topicPartition);
        if (count != null) {
            count.decrementAndGet();
        }
    }
    
    /**
     * Get the current in-flight batch count for a topic-partition
     * @param topicPartition The topic-partition to check
     * @return The number of in-flight batches (0 if none)
     */
    public int getInFlightCount(TopicPartition topicPartition) {
        AtomicInteger count = inFlightBatches.get(topicPartition);
        return count != null ? count.get() : 0;
    }
    
    /**
     * Re-enqueue a failed batch back to the front of the partition's queue
     * This preserves ordering for retries
     * @param topicPartition The topic-partition to re-enqueue to
     * @param batch The batch to re-enqueue
     */
    public void reenqueue(TopicPartition topicPartition, RecordBatch batch) {
        Deque<RecordBatch> deque = partitionBatches.computeIfAbsent(
            topicPartition, k -> new ArrayDeque<>());
        
        // Put batch back at front to preserve order
        deque.addFirst(batch);
        
        Logger.info("Re-enqueued batch for {} with retry count {}", 
                    topicPartition, batch.getRetryCount());
    }
}
