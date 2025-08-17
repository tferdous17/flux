package server.internal.storage;

import org.tinylog.Logger;
import producer.RecordBatch;
import proto.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Log represents the entire log for a single partition in Flux, and more specifically is composed of
 * multiple LogSegment objects in which each segment represents a chunk of the partition's data on disk.
 * Log, by extension, is essentially a manager for all these segments.
 */
public class Log {
    private String topic;
    private int partitionId; // id of which partition it belongs to
    private List<LogSegment> segments;
    private LogSegment activeSegment;
    private AtomicInteger currentOffset = new AtomicInteger(0);

    private int logStartOffset; // record offsets, not bytes
    private int logEndOffset;

    // By default, creating a Log will also instantiate 1 empty and mutable LogSegment (default record offset: 0)
    public Log(String topic, int partitionId) throws IOException {
        this.topic = topic;
        this.partitionId = partitionId;

        LogSegment segment = new LogSegment(topic, partitionId, 0);
        this.segments = new ArrayList<>();
        this.segments.add(segment);
        this.activeSegment = segment;

        this.logStartOffset = segments.getFirst().getStartOffset();
        this.logEndOffset = segments.getFirst().getEndOffset();
    }

    // Instantiate log with already-made segment
    public Log(LogSegment segment, int partitionId) {
        this.segments = new ArrayList<>();
        this.segments.add(segment);
        this.activeSegment = segment;
        this.partitionId = partitionId;

        this.logStartOffset = segment.getStartOffset();
        this.logEndOffset = segment.getEndOffset();
    }

    // Instantiate with already-made list of segments
    public Log(List<LogSegment> segments) {
        this.segments = segments;
    }

    private boolean canAppendRecordToSegment(byte[] record) {
        if (activeSegment == null || !activeSegment.isActive()) {
            return false;
        }
        return (activeSegment.getCurrentSizeInBytes() + record.length) <= activeSegment.getSegmentThresholdInBytes();
    }

    private LogSegment createNewSegment(int partitionId) throws IOException {
        LogSegment newSegment = new LogSegment(this.topic, partitionId, currentOffset.get());
        segments.add(newSegment);
        activeSegment = newSegment;
        System.out.println("CURRENT OFFSET IS: " + currentOffset.get());
        Logger.info("Created new log segment starting at offset {}", currentOffset.get());
        return newSegment;
    }

    public int appendRecordBatch(RecordBatch batch) throws IOException {
        if (batch == null || batch.getRecordCount() == 0 || batch.getCurrBatchSizeInBytes() == 0) {
            throw new IllegalArgumentException("Batch is empty");
        }
        int batchStartOffset = currentOffset.get();

        if (activeSegment == null) {
            activeSegment = createNewSegment(partitionId);
        }
        activeSegment.writeBatchToSegment(batch);

        // Update the offset after writing the batch
        currentOffset.addAndGet(batch.getRecordCount());

        Logger.info("Successfully appended {} records starting at offset {}", batch.getRecordCount(), batchStartOffset);

        return batchStartOffset;
    }

    public void appendSingleRecord(byte[] record, int currRecordOffset) throws IOException {
        if (activeSegment == null) {
            activeSegment = createNewSegment(partitionId);
        }

        Logger.info("APPEND SINGLE RECORD: " + Arrays.toString(record));
        // if writeRecordToSegment returns false, it means the active segment is no longer mutable
        if (!activeSegment.writeRecordToSegment(record, currRecordOffset)) {
            // thus we have to create a new active segment to append incoming records to
            createNewSegment(partitionId).writeRecordToSegment(record, currRecordOffset);
        };
        currentOffset.getAndAdd(1);
        this.logEndOffset = currentOffset.get();
        Logger.info("2. Successfully appended record to active segment");
    }

    public Message getRecordAtOffset(int recordOffset) throws IOException {
        if (activeSegment == null) {
            System.err.println("Segment is empty. Cannot get record.");
            return null;
        }
        return activeSegment.getRecordFromSegmentAtOffset(recordOffset);
    }

    public List<LogSegment> getAllLogSegments() {
        return this.segments;
    }

    public int getLogStartOffset() {
        return logStartOffset;
    }

    public int getLogEndOffset() {
        return logEndOffset;
    }

    @Override
    public String toString() {
        return "Log{" +
                ", \ncurrentActiveSegmentIdx=" +
                ", \nlogStartOffset=" + logStartOffset +
                ", \nlogEndOffset=" + logEndOffset +
                ", \ncurrentSizeInBytes=" +
                ", \nsegments=" + segments +
                '}';
    }
}
