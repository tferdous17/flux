package producer;

import commons.CompressionType;
import java.util.Properties;

/**
 * Configuration class for FluxProducer and RecordAccumulator
 * Provides simple batch configuration settings
 */
public class ProducerConfig {
    private final int batchSize;
    private final int lingerMs;
    private final CompressionType compressionType;
    private final long bufferMemory;
    private final long maxBlockMs;
    private final int retries;
    private final long deliveryTimeoutMs;
    private final int maxInFlightRequests;
    private final int maxRequestSize;
    
    // Default values 
    private static final int DEFAULT_BATCH_SIZE = 16384; // 16KB
    private static final int DEFAULT_LINGER_MS = 100; // 100ms
    private static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.NONE;
    private static final long DEFAULT_BUFFER_MEMORY = 33554432L; // 32MB
    private static final long DEFAULT_MAX_BLOCK_MS = 60000L; // 60 seconds
    private static final int DEFAULT_RETRIES = 3;
    private static final long DEFAULT_DELIVERY_TIMEOUT_MS = 120000L; // 120 seconds
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 5;
    private static final int DEFAULT_MAX_REQUEST_SIZE = 1048576; // 1MB
    
    /**
     * Create ProducerConfig with default values
     */
    public ProducerConfig() {
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.lingerMs = DEFAULT_LINGER_MS;
        this.compressionType = DEFAULT_COMPRESSION_TYPE;
        this.bufferMemory = DEFAULT_BUFFER_MEMORY;
        this.maxBlockMs = DEFAULT_MAX_BLOCK_MS;
        this.retries = DEFAULT_RETRIES;
        this.deliveryTimeoutMs = DEFAULT_DELIVERY_TIMEOUT_MS;
        this.maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        this.maxRequestSize = DEFAULT_MAX_REQUEST_SIZE;
    }
    
    /**
     * Create ProducerConfig from Properties
     * @param props Configuration properties
     */
    public ProducerConfig(Properties props) {
        this.batchSize = Integer.parseInt(
            props.getProperty("batch.size", String.valueOf(DEFAULT_BATCH_SIZE))
        );
        validateBatchSize(this.batchSize);
        
        this.lingerMs = Integer.parseInt(
            props.getProperty("linger.ms", String.valueOf(DEFAULT_LINGER_MS))
        );
        String compressionName = props.getProperty("compression.type", DEFAULT_COMPRESSION_TYPE.getName());
        CompressionType tempCompressionType;
        try {
            tempCompressionType = CompressionType.fromName(compressionName);
        } catch (IllegalArgumentException e) {
            tempCompressionType = DEFAULT_COMPRESSION_TYPE;
        }
        this.compressionType = tempCompressionType;
        this.bufferMemory = Long.parseLong(
            props.getProperty("buffer.memory", String.valueOf(DEFAULT_BUFFER_MEMORY))
        );
        this.maxBlockMs = Long.parseLong(
            props.getProperty("max.block.ms", String.valueOf(DEFAULT_MAX_BLOCK_MS))
        );
        this.retries = Integer.parseInt(
            props.getProperty("retries", String.valueOf(DEFAULT_RETRIES))
        );
        this.deliveryTimeoutMs = Long.parseLong(
            props.getProperty("delivery.timeout.ms", String.valueOf(DEFAULT_DELIVERY_TIMEOUT_MS))
        );
        this.maxInFlightRequests = Integer.parseInt(
            props.getProperty("max.in.flight.requests", String.valueOf(DEFAULT_MAX_IN_FLIGHT_REQUESTS))
        );
        this.maxRequestSize = Integer.parseInt(
            props.getProperty("max.request.size", String.valueOf(DEFAULT_MAX_REQUEST_SIZE))
        );
    }
    
    
    /**
     * Create ProducerConfig with all values including BufferPool settings
     */
    public ProducerConfig(int batchSize, int lingerMs, long bufferMemory, boolean compressionEnabled, long maxBlockMs) {
        validateBatchSize(batchSize);
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.compressionType = compressionEnabled ? CompressionType.GZIP : CompressionType.NONE;
        this.bufferMemory = bufferMemory;
        this.maxBlockMs = maxBlockMs;
        this.retries = DEFAULT_RETRIES;
        this.deliveryTimeoutMs = DEFAULT_DELIVERY_TIMEOUT_MS;
        this.maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        this.maxRequestSize = DEFAULT_MAX_REQUEST_SIZE;
    }
    
    public ProducerConfig(int batchSize, int lingerMs, long bufferMemory, CompressionType compressionType, long maxBlockMs) {
        validateBatchSize(batchSize);
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.compressionType = compressionType != null ? compressionType : DEFAULT_COMPRESSION_TYPE;
        this.bufferMemory = bufferMemory;
        this.maxBlockMs = maxBlockMs;
        this.retries = DEFAULT_RETRIES;
        this.deliveryTimeoutMs = DEFAULT_DELIVERY_TIMEOUT_MS;
        this.maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        this.maxRequestSize = DEFAULT_MAX_REQUEST_SIZE;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public int getLingerMs() {
        return lingerMs;
    }
    
    public CompressionType getCompressionType() {
        return compressionType;
    }
    
    public long getBufferMemory() {
        return bufferMemory;
    }
    
    public long getMaxBlockMs() {
        return maxBlockMs;
    }
    
    public int getRetries() {
        return retries;
    }
    
    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }
    
    public int getMaxInFlightRequests() {
        return maxInFlightRequests;
    }
    
    public int getMaxRequestSize() {
        return maxRequestSize;
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
}