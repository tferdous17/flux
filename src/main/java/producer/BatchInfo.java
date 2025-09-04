package producer;

import java.util.Objects;

/**
 * Immutable information about a batch for callback purposes.
 * This class provides a snapshot of batch metadata at the time of callback execution,
 * ensuring thread safety and preventing concurrent modification issues.
 */
public final class BatchInfo {
    private final String batchId;
    private final int partition;
    private final int recordCount;
    private final int batchSizeBytes;
    private final long createdTimeMs;
    private final Long sentTimeMs; // Nullable - only set when batch is sent
    private final String topicName;
    private final BatchState state;
    
    /**
     * Create a new BatchInfo instance.
     *
     * @param batchId Unique identifier for the batch
     * @param partition The partition this batch belongs to
     * @param recordCount Number of records in the batch
     * @param batchSizeBytes Size of the batch in bytes
     * @param createdTimeMs Timestamp when the batch was created
     * @param sentTimeMs Timestamp when the batch was sent (null if not yet sent)
     * @param topicName Name of the topic for this batch
     * @param state Current state of the batch
     */
    public BatchInfo(String batchId, int partition, int recordCount, int batchSizeBytes,
                     long createdTimeMs, Long sentTimeMs, String topicName, BatchState state) {
        this.batchId = Objects.requireNonNull(batchId, "batchId cannot be null");
        this.partition = partition;
        this.recordCount = recordCount;
        this.batchSizeBytes = batchSizeBytes;
        this.createdTimeMs = createdTimeMs;
        this.sentTimeMs = sentTimeMs;
        this.topicName = topicName;
        this.state = Objects.requireNonNull(state, "state cannot be null");
    }
    
    /**
     * @return Unique identifier for this batch
     */
    public String getBatchId() {
        return batchId;
    }
    
    /**
     * @return The partition number this batch belongs to
     */
    public int getPartition() {
        return partition;
    }
    
    /**
     * @return Number of records contained in this batch
     */
    public int getRecordCount() {
        return recordCount;
    }
    
    /**
     * @return Size of the batch in bytes
     */
    public int getBatchSizeBytes() {
        return batchSizeBytes;
    }
    
    /**
     * @return Timestamp when this batch was created (milliseconds since epoch)
     */
    public long getCreatedTimeMs() {
        return createdTimeMs;
    }
    
    /**
     * @return Timestamp when this batch was sent, or null if not yet sent
     */
    public Long getSentTimeMs() {
        return sentTimeMs;
    }
    
    /**
     * @return Name of the topic this batch belongs to
     */
    public String getTopicName() {
        return topicName;
    }
    
    /**
     * @return Current state of this batch
     */
    public BatchState getState() {
        return state;
    }
    
    /**
     * Calculate the age of this batch in milliseconds.
     *
     * @return Age of the batch since creation
     */
    public long getAgeMs() {
        return System.currentTimeMillis() - createdTimeMs;
    }
    
    /**
     * Calculate the send latency if the batch has been sent.
     *
     * @return Latency between creation and sending, or -1 if not yet sent
     */
    public long getSendLatencyMs() {
        return sentTimeMs != null ? sentTimeMs - createdTimeMs : -1;
    }
    
    /**
     * Create a copy of this BatchInfo with a new state.
     *
     * @param newState The new state for the batch
     * @return New BatchInfo instance with updated state
     */
    public BatchInfo withState(BatchState newState) {
        return new BatchInfo(batchId, partition, recordCount, batchSizeBytes,
                           createdTimeMs, sentTimeMs, topicName, newState);
    }
    
    /**
     * Create a copy of this BatchInfo with a sent timestamp.
     *
     * @param sentTimeMs The timestamp when the batch was sent
     * @return New BatchInfo instance with sent timestamp
     */
    public BatchInfo withSentTime(long sentTimeMs) {
        return new BatchInfo(batchId, partition, recordCount, batchSizeBytes,
                           createdTimeMs, sentTimeMs, topicName, state);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchInfo batchInfo = (BatchInfo) o;
        return partition == batchInfo.partition &&
               recordCount == batchInfo.recordCount &&
               batchSizeBytes == batchInfo.batchSizeBytes &&
               createdTimeMs == batchInfo.createdTimeMs &&
               Objects.equals(batchId, batchInfo.batchId) &&
               Objects.equals(sentTimeMs, batchInfo.sentTimeMs) &&
               Objects.equals(topicName, batchInfo.topicName) &&
               state == batchInfo.state;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(batchId, partition, recordCount, batchSizeBytes,
                          createdTimeMs, sentTimeMs, topicName, state);
    }
    
    @Override
    public String toString() {
        return "BatchInfo{" +
               "batchId='" + batchId + '\'' +
               ", partition=" + partition +
               ", recordCount=" + recordCount +
               ", batchSizeBytes=" + batchSizeBytes +
               ", createdTimeMs=" + createdTimeMs +
               ", sentTimeMs=" + sentTimeMs +
               ", topicName='" + topicName + '\'' +
               ", state=" + state +
               '}';
    }
}