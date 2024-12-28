package broker;

import org.tinylog.Logger;
import producer.ProducerRecord;

import java.io.File;
import java.io.IOException;

public class LogSegment {
    private final int partitionNumber;
    private File logFile; // stores the records
    private boolean isActive; // if isActive is true, the segment is mutable
    private long segmentThresholdInBytes =  1_048_576; // log segment cannot exceed this size threshold (default: 1 MB)
    private long currentSizeInBytes;
    private int startOffset; // for entire segment
    private int endOffset; // for entire segment

    public LogSegment(int partitionNumber, int startOffset) {
        this.partitionNumber = partitionNumber;
        this.startOffset = startOffset;

        try {
            // ex: partition1_000000.log, actual kafka names log files purely by byte offset like 000000123401.log
            this.logFile = new File(String.format("./data/partition%d_%05d.log", this.partitionNumber, this.startOffset));
            if (this.logFile.createNewFile()) {
                Logger.info(String.format("File created: %s%n", this.logFile.getPath()));
            } else {
                Logger.warn(String.format("File %s already exists", this.logFile.getPath()));
            }
        } catch (IOException | NullPointerException e) {
            Logger.error("Could not create LogSegment file");
            e.printStackTrace();
        }

        this.isActive = true; // by default, this LogSegment is active upon creation, but tread carefully
        this.currentSizeInBytes = 0;
    }

    // overloaded constructor incase we want to manually define threshold
    public LogSegment(int partitionNumber, int startOffset, long segmentThresholdInBytes) {
        this(partitionNumber, startOffset);
        this.segmentThresholdInBytes = segmentThresholdInBytes;
    }

    // once its immutable, the LogSegment can not be written to anymore (important)
    public void setAsImmutable() {
        this.isActive = false;
    }

    // TODO: Implement below method once RecordBatch is implemented
    public boolean writeBatchToSegment() {
        return true;
    }

    public boolean isActive() {
        return isActive;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public long getSegmentThresholdInBytes() {
        return segmentThresholdInBytes;
    }

    public long getCurrentSizeInBytes() {
        return currentSizeInBytes;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }
}
