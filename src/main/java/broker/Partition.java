package broker;
import producer.RecordBatch;
import proto.Message;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Partition {
    private Log log;
    private int partitionId;
    private AtomicInteger currentOffset = new AtomicInteger(0);


    public Partition(int partitionId) throws IOException {
        this.partitionId = partitionId;
        this.log = new Log(partitionId);
        this.currentOffset = new AtomicInteger(log.getLogEndOffset());
    }

    public void appendSingleRecord(byte[] record, int currRecordOffset) throws IOException {
        if (record.length == 0) {
            return;
        }
        this.log.appendSingleRecord(record, currRecordOffset);
    }

    public Message getRecordAtOffset(int recordOffset) throws IOException {
        return this.log.getRecordAtOffset(recordOffset);
    }

    public int appendRecordBatch(RecordBatch batch) throws IOException {
        return this.log.appendRecordBatch(batch);
    }

    public int getCurrentOffset() {
        return currentOffset.get();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Log getLog() {
        return log;
    }

    public void setCurrentOffset(AtomicInteger currentOffset) {
        this.currentOffset = currentOffset;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setLog(Log log) {
        this.log = log;
    }
}