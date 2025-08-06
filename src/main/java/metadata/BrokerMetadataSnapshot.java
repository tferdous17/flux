package metadata;

/**
 * Represents an immutable snapshot of the broker metadata
 */
public class BrokerMetadataSnapshot {
    private String brokerId;
    private String host;
    private int portNumber;
    private int numPartitions;

    public BrokerMetadataSnapshot(String brokerId, String host, int portNumber, int numPartitions) {
        this.brokerId = brokerId;
        this.host = host;
        this.portNumber = portNumber;
        this.numPartitions = numPartitions;
    }

    static BrokerMetadataSnapshot empty() {
        return new BrokerMetadataSnapshot(null, null, -1, -1);
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    @Override
    public String toString() {
        return "BrokerMetadataSnapshot [brokerId=" + brokerId + ", host=" + host + ", portNumber=" + portNumber + ", numPartitions=" + numPartitions + "]";
    }
}
