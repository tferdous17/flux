package producer;

import java.util.Properties;

/**
 * Configuration class for FluxProducer and RecordAccumulator
 * Provides simple batch configuration settings
 */
public class ProducerConfig {
    private final int batchSize;
    private final int lingerMs;
    private final int maxBufferSize;
    private final boolean compressionEnabled;
    
    // Default values
    private static final int DEFAULT_BATCH_SIZE = 16384; // 16KB
    private static final int DEFAULT_LINGER_MS = 100; // 100ms
    private static final int DEFAULT_MAX_BUFFER_SIZE = 33554432; // 32MB
    private static final boolean DEFAULT_COMPRESSION_ENABLED = true;
    
    /**
     * Create ProducerConfig with default values
     */
    public ProducerConfig() {
        this.batchSize = DEFAULT_BATCH_SIZE;
        this.lingerMs = DEFAULT_LINGER_MS;
        this.maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        this.compressionEnabled = DEFAULT_COMPRESSION_ENABLED;
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
        this.maxBufferSize = Integer.parseInt(
            props.getProperty("max.buffer.size", String.valueOf(DEFAULT_MAX_BUFFER_SIZE))
        );
        this.compressionEnabled = Boolean.parseBoolean(
            props.getProperty("compression.enabled", String.valueOf(DEFAULT_COMPRESSION_ENABLED))
        );
    }
    
    /**
     * Create ProducerConfig with custom values
     */
    public ProducerConfig(int batchSize, int lingerMs, int maxBufferSize, boolean compressionEnabled) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.maxBufferSize = maxBufferSize;
        this.compressionEnabled = compressionEnabled;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public int getLingerMs() {
        return lingerMs;
    }
    
    public int getMaxBufferSize() {
        return maxBufferSize;
    }
    
    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }
}