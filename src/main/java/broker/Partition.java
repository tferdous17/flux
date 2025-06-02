package broker;
import org.tinylog.Logger;
import producer.RecordBatch;
import proto.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Partition {
    private Log log;
    private int partitionId;
    private AtomicInteger currentOffset = new AtomicInteger(0);

    private List<LogSegment> segments;
    private LogSegment activeSegment;

    public Partition(int partitionId) throws IOException {
        // figure out why other log constructor is not creating partition file
        this.partitionId = partitionId;
        this.segments = new ArrayList<>();
        LogSegment segment = createNewSegment();
        this.activeSegment = segment;
        this.log = new Log(segment);
        this.currentOffset = new AtomicInteger(log.getLogEndOffset());
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
        System.out.println("CURRENT OFFSET IS: " + currentOffset.get());
        Logger.info("Created new log segment starting at offset {}", currentOffset.get());
        return newSegment;
    }

    public void appendSingleRecord(byte[] record, int currRecordOffset) throws IOException {
        if (activeSegment == null) {
            activeSegment = createNewSegment();
        }

        Logger.info("APPEND SINGLE RECORD: " + Arrays.toString(record));
        // if writeRecordToSegment returns false, it means the active segment is no longer mutable
        if (!activeSegment.writeRecordToSegment(record, currRecordOffset)) {
            // thus we have to create a new active segment to append incoming records to
            createNewSegment().writeRecordToSegment(record, currRecordOffset);
        };
        currentOffset.getAndAdd(1);
        Logger.info("2. Successfully appended record to active segment");
    }

    public Message getRecordAtOffset(int recordOffset) throws IOException {
        if (activeSegment == null) {
            System.err.println("Segment is empty. Cannot get record.");
            return null;
        }
        return activeSegment.getRecordFromSegmentAtOffset(recordOffset);
    }

    public int appendRecordBatch(RecordBatch batch) throws IOException {
        if (batch == null || batch.getRecordCount() == 0 || batch.getCurrBatchSizeInBytes() == 0) {
            throw new IllegalArgumentException("Batch is empty");
        }
        int batchStartOffset = currentOffset.get();

        if (activeSegment == null) {
            activeSegment = createNewSegment();
        }
        activeSegment.writeBatchToSegment(batch);

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