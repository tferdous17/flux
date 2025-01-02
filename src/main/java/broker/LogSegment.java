package broker;

import org.tinylog.Logger;
import producer.RecordBatch;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A LogSegment is a component/storage unit that makes up a Flux Partition.
 * Each segment holds a certain amount of records in sequential manner, typically in the form of batches.
 * Upon reaching the defined byte threshold, a segment will automatically become immutable.
 */
public class LogSegment {
    private final int partitionNumber;
    private File logFile; // stores the records
    private boolean isActive; // if isActive is true, the segment is mutable
    private int segmentThresholdInBytes =  1_048_576; // log segment cannot exceed this size threshold (default: 1 MB)
    private int currentSizeInBytes;
    private int startOffset; // for entire segment
    private int endOffset; // for entire segment

    public LogSegment(int partitionNumber, int startOffset) throws IOException {
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
        } catch (IOException e) {
            Logger.error("IOException occurred while creating LogSegment file.");
            throw e;
        }

        this.isActive = true; // by default, this LogSegment is active upon creation, but tread carefully
        this.currentSizeInBytes = 0;
    }

    // overloaded constructor in case we want to manually define threshold
    public LogSegment(int partitionNumber, int startOffset, int segmentThresholdInBytes) throws IOException {
        this(partitionNumber, startOffset);
        this.segmentThresholdInBytes = segmentThresholdInBytes;
    }

    public boolean shouldBeImmutable() {
        return this.currentSizeInBytes >= segmentThresholdInBytes;
    }

    // once its immutable, the LogSegment can not be written to anymore (important)
    public void setAsImmutable() {
        this.isActive = false;
    }

    public void writeBatchToSegment(RecordBatch batch) throws IOException {
        if (shouldBeImmutable()) {
            Logger.info("Log segment is now at capacity, setting as immutable.");
            setAsImmutable();
            return;
        }
        if (!isActive || batch.getCurrBatchSizeInBytes() == 0 || batch == null) {
            Logger.warn("Batch either immutable, empty, or null.");
            return;
        }
        if (this.currentSizeInBytes + batch.getCurrBatchSizeInBytes() > segmentThresholdInBytes) {
            Logger.warn("Size of batch exceeds log segment threshold.");
            return;
        }

        // grab the buffer and throw the occupied space in the buffer to a byte arr that will be written to disk
        ByteBuffer buffer = batch.getBatchBuffer().flip();
        byte[] occupiedData = new byte[batch.getCurrBatchSizeInBytes()];
        buffer.get(occupiedData); // transfers bytes from buffer --> occupiedData

        try {
            // write occupied data to the log file
            Files.write(Path.of(logFile.getPath()), occupiedData, StandardOpenOption.APPEND);
            this.currentSizeInBytes += occupiedData.length;
            Logger.info("Batch successfully written to segment.");
        } catch (IOException e) {
            Logger.error("Failed to write batch to log segment.", e);
            throw e;
        }
    }

    public boolean isActive() {
        return isActive;
    }

    public File getLogFile() {
        return logFile;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public int getSegmentThresholdInBytes() {
        return segmentThresholdInBytes;
    }

    public int getCurrentSizeInBytes() {
        return currentSizeInBytes;
    }

    public int getStartOffset() {
        return startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "LogSegment{" +
                "\npartitionNumber=" + partitionNumber +
                ", \nlogFile=" + logFile +
                ", \nisActive=" + isActive +
                ", \nsegmentThresholdInBytes=" + segmentThresholdInBytes +
                ", \ncurrentSizeInBytes=" + currentSizeInBytes +
                ", \nstartOffset=" + startOffset +
                ", \nendOffset=" + endOffset +
                '}';
    }
}
