package producer;

/**
 * Represents the lifecycle states of a RecordBatch in the accumulator system.
 * Batches progress through these states in order, with FAILED being a terminal
 * state that can be reached from any other state.
 */
public enum BatchState {
    /**
     * Batch has been created and is ready to accept records.
     * This is the initial state for all new batches.
     */
    CREATED("Batch created and accepting records"),
    
    /**
     * Batch is ready for sending due to size limit or time expiration.
     * No more records can be added to batches in this state.
     */
    READY("Batch ready for sending"),
    
    /**
     * Batch is currently being sent to the broker.
     * The network request is in progress.
     */
    SENDING("Batch being sent to broker"),
    
    /**
     * Batch has been successfully sent and acknowledged by the broker.
     * This is a terminal state indicating success.
     */
    COMPLETED("Batch successfully completed"),
    
    /**
     * Batch failed to send or was rejected by the broker.
     * This is a terminal state indicating failure.
     */
    FAILED("Batch failed to send");
    
    private final String description;
    
    BatchState(String description) {
        this.description = description;
    }
    
    /**
     * @return Human-readable description of this batch state
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Check if this state is a terminal state (no further transitions possible).
     *
     * @return true if this is a terminal state (COMPLETED or FAILED)
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }
    
    /**
     * Check if this state allows adding more records.
     *
     * @return true if records can be added (only CREATED state)
     */
    public boolean canAcceptRecords() {
        return this == CREATED;
    }
    
    /**
     * Check if this state indicates the batch can be sent.
     *
     * @return true if the batch is ready to send (READY or SENDING states)
     */
    public boolean canBeSent() {
        return this == READY || this == SENDING;
    }
    
    /**
     * Validate if a transition from this state to another state is allowed.
     *
     * @param newState The target state for transition
     * @return true if the transition is valid
     * @throws IllegalStateException if the transition is not allowed
     */
    public boolean canTransitionTo(BatchState newState) {
        // Terminal states cannot transition to other states
        if (this.isTerminal()) {
            return false;
        }
        
        // Valid transitions based on lifecycle
        switch (this) {
            case CREATED:
                return newState == READY || newState == FAILED;
            case READY:
                return newState == SENDING || newState == FAILED;
            case SENDING:
                return newState == COMPLETED || newState == FAILED;
            default:
                return false;
        }
    }
    
    /**
     * Validate a state transition and throw an exception if invalid.
     *
     * @param newState The target state for transition
     * @throws IllegalStateException if the transition is not allowed
     */
    public void validateTransition(BatchState newState) {
        if (!canTransitionTo(newState)) {
            throw new IllegalStateException(
                String.format("Invalid batch state transition from %s to %s", this, newState));
        }
    }
    
    @Override
    public String toString() {
        return name() + " (" + description + ")";
    }
}