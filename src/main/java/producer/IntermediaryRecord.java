package producer;

/**
 * Represents an intermediary form of our records as it gets passed through the system.
 *
 * This form makes it easier to track metadata (target partition ID) for newly serialized records without
 * having to prematurely de-serialize them once they reach the broker just to extract said metadata.
 * @param targetPartition id of partition to send this record to
 * @param data the record in serialized form
 */
public record IntermediaryRecord(int targetPartition, byte[] data) {
}
