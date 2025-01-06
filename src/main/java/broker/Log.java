package broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Log represents the entire log for a single partition in Flux, and more specifically is composed of
 * multiple LogSegment objects in which each segment represents a chunk of the partition's data on disk.
 * Log, by extension, is essentially a manager for all these segments.
 */
public class Log {
    private List<LogSegment> segments;
    private int currentActiveSegmentIdx = 0; // references index in above list
    private int logStartOffset; // record offsets, not bytes
    private int logEndOffset;
    private int currentSizeInBytes;

    // By default, creating a Log will also instantiate 1 empty and mutable LogSegment (default record offset: 0)
    public Log() throws IOException {
        this.segments = new ArrayList<>();
        try {
            this.segments.add(new LogSegment(0, 0));
            this.logStartOffset = segments.get(0).getStartOffset();
            this.logEndOffset = segments.get(0).getEndOffset();
        } catch (IOException e) {
            throw e;
        }
    }

    // Instantiate log with already-made segment
    public Log(LogSegment segment) {
        this.segments = new ArrayList<>();
        this.segments.add(segment);
        this.logStartOffset = segment.getStartOffset();
        this.logEndOffset = segment.getEndOffset();
    }

    // Instantiate with already-made list of segments
    public Log(List<LogSegment> segments) {
        this.segments = segments;
    }

    // TODO: Implement when RecordBatch is made
    public void appendRecordBatch() {
        LogSegment activeSegment = this.getCurrentActiveLogSegment();
    }

    // TODO: Implement when ProducerRecord serialization is implemented
    public void appendSingleRecord() {
        // always append to the current active segment
        LogSegment activeSegment = this.getCurrentActiveLogSegment();
    }

    public LogSegment getCurrentActiveLogSegment() {
        return this.segments.get(currentActiveSegmentIdx);
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

    public int getCurrentSizeInBytes() {
        return this.currentSizeInBytes;
    }

    @Override
    public String toString() {
        return "Log{" +
                ", \ncurrentActiveSegmentIdx=" + currentActiveSegmentIdx +
                ", \nlogStartOffset=" + logStartOffset +
                ", \nlogEndOffset=" + logEndOffset +
                ", \ncurrentSizeInBytes=" + currentSizeInBytes +
                ", \nsegments=" + segments +
                '}';
    }
}
