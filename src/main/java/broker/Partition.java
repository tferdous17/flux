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
        LogSegment activeSegment = log.getCurrentActiveLogSegment(); //Fetches Current active segment as a commented
        if (!activeSegment.isActive()) {
            return false;
        }
        return (activeSegment.getCurrentSizeInBytes() + record.length) <= activeSegment.getSegmentThresholdInBytes();
    }

    private LogSegment createNewSegment() throws IOException {
        LogSegment newSegment = new LogSegment(partitionId, currentOffset.get());
        log.getAllLogSegments().add(newSegment);

        Logger.info("Created new log segment starting at offset {}", currentOffset.get());
        return newSegment;
    }

    private void appendRecordToSegment( byte[] record) {
        LogSegment activeSegment = log.getCurrentActiveLogSegment();
        activeSegment.setCurrentSizeInBytes(activeSegment.getCurrentSizeInBytes() + record.length);

        int newOffset = currentOffset.incrementAndGet();
        activeSegment.setEndOffset(newOffset);

        Logger.debug("Appended record at offset {}", newOffset - 1);
    }

    public int appendRecordBatch(RecordBatch batch) throws IOException {
        if (batch == null || batch.getBatch().isEmpty()) {
            throw new IllegalArgumentException("Batch is empty");
        }
        int batchStartOffset = currentOffset.get();
        LogSegment activeSegment = log.getCurrentActiveLogSegment();

        if (activeSegment.getCurrentSizeInBytes() + batch.getCurrBatchSizeInBytes() > activeSegment.getSegmentThresholdInBytes()) {
            activeSegment = createNewSegment();
        }
        activeSegment.writeBatchToSegment(batch); // My ide doesn't have the current writebatchsegment code so this will give an error

        currentOffset.addAndGet(batch.getRecordCount());

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