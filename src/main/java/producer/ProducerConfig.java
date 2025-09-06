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
    
    // Default values 
    private static final int DEFAULT_BATCH_SIZE = 16384; // 16KB
    private static final int DEFAULT_LINGER_MS = 100; // 100ms
    private static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.NONE;
    private static final long DEFAULT_BUFFER_MEMORY = 33554432L; // 32MB
    private static final long DEFAULT_MAX_BLOCK_MS = 60000L; // 60 seconds
    
    /**
     * Create ProducerConfig with default values
     */
    public ProducerConfig() {
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.lingerMs = DEFAULT_LINGER_MS;
        this.compressionType = DEFAULT_COMPRESSION_TYPE;
        this.bufferMemory = DEFAULT_BUFFER_MEMORY;
        this.maxBlockMs = DEFAULT_MAX_BLOCK_MS;
    }
    
    /**
     * Create ProducerConfig from Properties
     * @param props Configuration properties
     */
    public ProducerConfig(Properties props) {
        this.batchSize = Integer.parseInt(
            props.getProperty("batch.size", String.valueOf(DEFAULT_BATCH_SIZE))
        );
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
    }
    
    
    /**
     * Create ProducerConfig with all values including BufferPool settings
     */
    public ProducerConfig(int batchSize, int lingerMs, long bufferMemory, boolean compressionEnabled, long maxBlockMs) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.compressionType = compressionEnabled ? CompressionType.GZIP : CompressionType.NONE;
        this.bufferMemory = bufferMemory;
        this.maxBlockMs = maxBlockMs;
    }
    
    public ProducerConfig(int batchSize, int lingerMs, long bufferMemory, CompressionType compressionType, long maxBlockMs) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.compressionType = compressionType != null ? compressionType : DEFAULT_COMPRESSION_TYPE;
        this.bufferMemory = bufferMemory;
        this.maxBlockMs = maxBlockMs;
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
}