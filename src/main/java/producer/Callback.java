package producer;

/**
 * A callback interface that the user can implement to handle request completion.
 * This callback will be invoked when the record sent to the server has been acknowledged.
 * 
 * Exactly one of the arguments will be non-null:
 * - metadata will be non-null if the send was successful
 * - exception will be non-null if the send failed
 * 
 * This interface matches Kafka's producer callback pattern for compatibility.
 */
public interface Callback {
    
    /**
     * Called when the record has been acknowledged by the server.
     * 
     * @param metadata The metadata for the record that was sent (non-null if successful)
     * @param exception The exception thrown during processing (non-null if failed)
     */
    void onCompletion(RecordMetadata metadata, Exception exception);
}