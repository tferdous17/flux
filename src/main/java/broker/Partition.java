package broker;
import org.tinylog.Logger;
import producer.RecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Partition {
    private Log log;
    private int partitionId;
    private AtomicInteger currentOffset;

    private List<LogSegment> segments;
    private LogSegment activeSegment;


    public Partition( int partitionId) throws IOException {
        this.log = new Log();
        this.partitionId = partitionId;
        this.currentOffset = new AtomicInteger(log.getLogEndOffset());
        // Initialize the segments list and set the first active segment
        this.segments = new ArrayList<>();
        this.activeSegment = createNewSegment();
    }

    private boolean canAppendRecordToSegment(byte[] record) {
        if (activeSegment == null || !activeSegment.isActive()) {
            return false;
        }
        return (activeSegment.getCurrentSizeInBytes() + record.length) <= activeSegment.getSegmentThresholdInBytes();
    }

    private LogSegment createNewSegment() throws IOException {
        LogSegment newSegment = new LogSegment(partitionId, currentOffset.get());
        segments.add(newSegment);
        activeSegment = newSegment;
        Logger.info("Created new log segment starting at offset {}", currentOffset.get());
        return newSegment;
    }


    public int appendRecordBatch(RecordBatch batch) throws IOException {
        if (batch == null || batch.getRecordCount() == 0 || batch.getCurrBatchSizeInBytes() == 0) {
            throw new IllegalArgumentException("Batch is empty");
        }
        int batchStartOffset = currentOffset.get();

        if (activeSegment != null) {
            activeSegment.writeBatchToSegment(batch);
        } else {
            activeSegment = createNewSegment();
            activeSegment.writeBatchToSegment(batch);
        }
        // Update the offset after writing the batch
        currentOffset.addAndGet(batch.getRecordCount());

        Logger.info("Successfully appended {} records starting at offset {}", batch.getRecordCount(), batchStartOffset);

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
    public List<LogSegment> getSegments() {
        return segments;
    }
}