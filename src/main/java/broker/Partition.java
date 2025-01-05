package broker;
import org.tinylog.Logger;
import producer.RecordBatch;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Partition {
    private Log log;
    private int partitionId;
    private AtomicInteger currentOffset;

    public Partition( int partitionId) throws IOException {
        this.log = new Log();
        this.partitionId = partitionId;
        this.currentOffset = new AtomicInteger(log.getLogEndOffset());
    }

    private boolean canAppendRecordToSegment( byte[] record) {
        LogSegment activeSegment1 = log.getCurrentActiveLogSegment(); //Fetches Current active segment as a commented
        if (!activeSegment1.isActive()) {
            return false;
        }
        return (activeSegment1.getCurrentSizeInBytes() + record.length) <= activeSegment1.getSegmentThresholdInBytes();
    }

    private LogSegment createNewSegment() throws IOException {
        LogSegment newSegment = new LogSegment(partitionId, currentOffset.get());
        log.getAllLogSegments().add(newSegment);

        Logger.info("Created new log segment starting at offset {}", currentOffset.get());
        return newSegment;
    }

    private void appendRecordToSegment(LogSegment segment, byte[] record) {

     //   segment.

      //  Logger.debug("Appended record at offset {}", newOffset - 1);
    }

    public int appendRecordBatch(RecordBatch batch) throws IOException {
        if (batch == null || batch.getBatch().isEmpty()) {
            throw new IllegalArgumentException("Cannot append null or empty batch");
        }

        int batchStartOffset = currentOffset.get();
        LogSegment activeSegment = log.getCurrentActiveLogSegment();

        for (byte[] record : batch.getBatch()) {
            if (!canAppendRecordToSegment(activeSegment, record)) {
                activeSegment = createNewSegment();
            }
            appendRecordToSegment(activeSegment, record);
        }

        Logger.info("Successfully appended {} records starting at offset {}",
                batch.getRecordCount(), batchStartOffset);
        return batchStartOffset;
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
}