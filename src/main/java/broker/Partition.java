package broker;
import org.tinylog.Logger;
import producer.RecordBatch;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extended LogSegment that provides access to private fields we need
 */
class ExtendedLogSegment extends LogSegment {
    public ExtendedLogSegment(int partitionNumber, int startOffset) throws IOException {
        super(partitionNumber, startOffset);
    }

    public long currentSizeInBytes;
    public int endOffset;
}

public class Partition {
    private Log log;
    private int partitionId;
    private AtomicInteger currentOffset;
    private static final long MAX_SEGMENT_SIZE = 1024;

    public Partition(Log log, int partitionId) {
        this.log = log;
        this.partitionId = partitionId;
        this.currentOffset = new AtomicInteger(log.getLogEndOffset());
    }

    private boolean canAppendRecordToSegment(LogSegment segment, byte[] record) {
        if (!segment.isActive()) {
            return false;
        }
        return (segment.getCurrentSizeInBytes() + record.length) <= MAX_SEGMENT_SIZE;
    }

    private LogSegment createNewSegment() throws IOException {
        ExtendedLogSegment newSegment = new ExtendedLogSegment(partitionId, currentOffset.get());
        log.getAllLogSegments().add(newSegment);

        Logger.info("Created new log segment starting at offset {}", currentOffset.get());
        return newSegment;
    }

    private void appendRecordToSegment(LogSegment segment, byte[] record) {
        ExtendedLogSegment extSegment = (ExtendedLogSegment) segment;
        extSegment.currentSizeInBytes += record.length;

        int newOffset = currentOffset.incrementAndGet();
        extSegment.endOffset = newOffset;

        Logger.debug("Appended record at offset {}", newOffset - 1);
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