package broker;

import consumer.ConsumerRecord;
import org.tinylog.Logger;
import producer.RecordBatch;

import java.io.IOException;

public class Broker {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions = 1; // for now, we will only support 1 partition
    private Partition partition;

    public Broker() throws IOException {
        this.brokerId = "broker1";
        this.host = "localhost";
        this.port = 8080;
        this.partition = new Partition(1);
    }

    public Broker(String brokerId, String host, int port) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.partition = new Partition(1);
    }

    public boolean start() {
        // starts the server via gRPC
        return true;
    }

    // mock implementation for now until gRPC implemented
    public void produceMessages(RecordBatch batch) throws IOException {
        partition.appendRecordBatch(batch);
        Logger.info("Appended record batch to broker.");
    }

    // TODO: Finish consumer infrastructure
    public ConsumerRecord consumeMessage() {
        return null;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public Partition getPartition() {
        return partition;
    }
}
