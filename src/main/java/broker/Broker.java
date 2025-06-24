package broker;

import org.tinylog.Logger;
import producer.RecordBatch;
import proto.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class Broker {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions = 1; // for now, we will only support 1 partition
    private Partition partition;
    private int nextAvailOffset; // record offsets

    public Broker(String brokerId, String host, int port) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.partition = new Partition(1);
        this.nextAvailOffset = 0;
    }

    public Broker() throws IOException {
        this("BROKER-1", "localhost", 50051);
    }

    public int produceSingleMessage(byte[] record) throws IOException {
        // throw record offset into the header (first 4 bytes)
        ByteBuffer buffer = ByteBuffer.wrap(record);
        buffer.putInt(0, nextAvailOffset);

        Logger.info("PRODUCE SINGLE MSSAGE: " + Arrays.toString(buffer.array()));

        int currRecordOffset = nextAvailOffset;
        nextAvailOffset++;

        partition.appendSingleRecord(record, currRecordOffset);
        Logger.info("1. Appended record to broker.");

        return currRecordOffset;
    }

    // TODO: Replace mock implementation when gRPC is implemented
    public void produceMessages(RecordBatch batch) throws IOException {
        partition.appendRecordBatch(batch);
        Logger.info("Appended record batch to broker.");
    }

    public int produceMessages(List<byte[]> messages) throws IOException {
        // we can just call the produceSingleMessage() for each byte[] in messages
        int counter = 0;
        int lastRecordOffset = nextAvailOffset;
        for (byte[] message : messages) {
            lastRecordOffset = produceSingleMessage(message);
            counter++;
        }
        Logger.info("Appended " + counter + " records to broker.");
        return lastRecordOffset;
    }

    // TODO: Finish consumer infrastructure
    public Message consumeMessage(int startingOffset) throws IOException {
        // peer into the partition and fetch the data at this particular offset
        return partition.getRecordAtOffset(startingOffset);
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
