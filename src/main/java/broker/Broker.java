package broker;

import consumer.ConsumerRecord;
import org.tinylog.Logger;
import producer.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Broker {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions = 1; // for now, we will only support 1 partition
    private Partition partition;
    private int nextAvailOffset; // record offsets

    public Broker() throws IOException {
        this.brokerId = "BROKER-1";
        this.host = "localhost";
        this.port = 50051;
        this.partition = new Partition(1);
        this.nextAvailOffset = 0;
    }

    public Broker(String brokerId, String host, int port) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.partition = new Partition(1);
    }

    // TODO: Replace mock implementation when gRPC is implemented
    public void produceMessages(RecordBatch batch) throws IOException {
        partition.appendRecordBatch(batch);
        Logger.info("Appended record batch to broker.");
    }

    public int produceSingleMessage(byte[] record) throws IOException {
        // throw record offset into the header (first 4 bytes)
        ByteBuffer buffer = ByteBuffer.wrap(record);
        buffer.putInt(0, nextAvailOffset);

        int currRecordOffset = nextAvailOffset;
        nextAvailOffset++;

        partition.appendSingleRecord(record);
        Logger.info("1. Appended record to broker.");

        return currRecordOffset;
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
