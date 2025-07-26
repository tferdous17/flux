package exceptions;

public class InvalidPartitionException extends RuntimeException {

    public InvalidPartitionException(String exceptionMsg) {
        super(exceptionMsg);
    }

    public InvalidPartitionException(int invalidPartition) {
        super("Partition with id = " + invalidPartition + " either does not exist or is out of range");
    }

    public InvalidPartitionException(int invalidPartition, String topicName) {
        super("Partition with id = " + invalidPartition + " either does not exist or is out of range for topic = {" + topicName + "}");
    }
}
