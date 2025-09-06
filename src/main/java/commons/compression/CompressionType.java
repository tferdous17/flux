package commons.compression;

/**
 * Simple compression types for record batches, following Kafka's approach.
 * 
 * TODO: Future compression types to consider:
 * - SNAPPY: Fast compression with moderate ratio, good for balanced workloads
 * - LZ4: Very fast compression, ideal for high throughput scenarios
 * - ZSTD: Best compression ratio with tunable levels, modern algorithm
 * These would require external dependencies and performance testing.
 */
public enum CompressionType {
    /**
     * No compression applied.
     */
    NONE(0, "none"),
    
    /**
     * GZIP compression. Good compression ratio, built into Java.
     */
    GZIP(1, "gzip");
    
    private final int id;
    private final String name;
    
    CompressionType(int id, String name) {
        this.id = id;
        this.name = name;
    }
    
    /**
     * Get the numeric ID for this compression type.
     * Used in protobuf serialization.
     */
    public int getId() {
        return id;
    }
    
    /**
     * Get the string name for this compression type.
     * Used in configuration files.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get CompressionType from string name (case insensitive).
     * 
     * @param name The compression type name
     * @return The CompressionType enum value
     * @throws IllegalArgumentException if name is not recognized
     */
    public static CompressionType fromName(String name) {
        if (name == null) {
            return NONE;
        }
        
        for (CompressionType type : values()) {
            if (type.name.equalsIgnoreCase(name.trim())) {
                return type;
            }
        }
        
        throw new IllegalArgumentException(
            "Unknown compression type: " + name + ". Supported types: none, gzip, snappy, lz4"
        );
    }
    
    /**
     * Get CompressionType from numeric ID.
     * 
     * @param id The compression type ID
     * @return The CompressionType enum value
     * @throws IllegalArgumentException if id is not recognized
     */
    public static CompressionType fromId(int id) {
        for (CompressionType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        
        throw new IllegalArgumentException(
            "Unknown compression type ID: " + id
        );
    }
    
    @Override
    public String toString() {
        return name;
    }
}