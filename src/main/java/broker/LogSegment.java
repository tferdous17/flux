package broker;

import com.google.protobuf.ByteString;
import consumer.ConsumerRecord;
import org.tinylog.Logger;
import producer.ProducerRecord;
import producer.ProducerRecordCodec;
import producer.RecordBatch;
import proto.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A LogSegment is a component/storage unit that makes up a Flux Partition.
 * Each segment holds a certain amount of records in sequential manner, typically in the form of batches.
 * Upon reaching the defined byte threshold, a segment will automatically become immutable.
 */
public class LogSegment {
    private final int partitionNumber;
    private File logFile; // stores the records
    private File indexFile;
    private boolean isActive; // if isActive is true, the segment is mutable
    private int segmentThresholdInBytes =  1_048_576; // log segment cannot exceed this size threshold (default: 1 MB)
    private int currentSizeInBytes;
    private int startOffset; // for entire segment
    private int endOffset; // for entire segment
    private IndexEntries entries;

    /**
     * Represents a Record Offset --> Byte Offset pair
     * NOTE: Full implementation depends on global record offset management to be implemented
     */
    private class IndexEntries {
        public Map<Integer, Integer> recordOffsetToByteOffsets;
        private final int flushThreshold = 100; // note: contents will be empty after hitting this threshold

        public IndexEntries() {
            recordOffsetToByteOffsets = new HashMap<>();
        }

        // creates new entry and also handles automatic flushing
        public void createNewEntry(int recordOffset, int byteOffset) {
            recordOffsetToByteOffsets.put(recordOffset, byteOffset);
            if (recordOffsetToByteOffsets.size() >= flushThreshold) {
                try {
                    flushIndexEntries();
                    recordOffsetToByteOffsets.clear();
                }
                catch (IOException e) {
                    Logger.error("Error when flushing index entries.");
                    e.printStackTrace();
                }
            }
        }

        public void flushIndexEntries() throws IOException {
            // 8 bytes per entry (4 for record offset, 4 for byte offset)
            int numOfOffsetPairs = recordOffsetToByteOffsets.size();
            ByteBuffer buffer = ByteBuffer.allocate(numOfOffsetPairs * 8);

            recordOffsetToByteOffsets.forEach((recOffset, byteOffset) -> {
                buffer.putInt(recOffset);
                buffer.putInt(byteOffset);
            });

            Files.write(Path.of(indexFile.getPath()), buffer.array(), StandardOpenOption.APPEND);
        }
    }

    public LogSegment(int partitionNumber, int startOffset) throws IOException {
        this.partitionNumber = partitionNumber;
        this.startOffset = startOffset;
        entries = new IndexEntries();

        try {
            createDataFolder();

            // ex: partition1_000000.log, actual kafka names log files purely by byte offset like 000000123401.log
            String logFileName = String.format("data/partition%d_%05d.log", this.partitionNumber, this.startOffset);
            String indexFileName = String.format("data/partition%d_%05d.index", this.partitionNumber, this.startOffset);

            this.logFile = createFile(logFileName);
            this.indexFile = createFile(indexFileName);

        } catch (IOException e) {
            Logger.error("IOException occurred while creating LogSegment files.");
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

    private void createDataFolder() {
        // Get the current working directory (project root in most cases)
        String projectRoot = System.getProperty("user.dir");

        // Define the path for the data folder at the same level as src
        String dataFolderPath = Paths.get(projectRoot, "data").toString();

        // Create a File object for the data folder
        File dataFolder = new File(dataFolderPath);

        // Check if the folder exists, create it if it doesn't
        if (!dataFolder.exists()) {
            try {
                Files.createDirectories(Paths.get(dataFolderPath));
            } catch (IOException e) {
                throw new RuntimeException("Could not create data folder", e);
            }
        } else {
            System.out.println("data/ folder already exists at: " + dataFolderPath);
        }
    }

    // Helper method
    private File createFile(String fileName) throws IOException {
        File file = new File(fileName);
        try {
            if (file.createNewFile()) {
                Logger.info("File created: " + file.getPath());
            } else {
                // wipes existing content
                Files.write(Paths.get(fileName), "".getBytes());
                Logger.warn("File already exists: " + file.getPath());
            }
        } catch (IOException e) {
            throw e;
        }
        return file;
    }

    public boolean shouldBeImmutable() {
        return this.currentSizeInBytes >= segmentThresholdInBytes;
    }

    // once its immutable, the LogSegment can not be written to anymore (important)
    public void setAsImmutable() {
        this.isActive = false;
    }

    public void writeRecordToSegment(byte[] record, int currRecordOffset) throws IOException {
        if (shouldBeImmutable()) {
            Logger.info("Log segment is now at capacity, setting as immutable.");
            setAsImmutable();
            return;
        }
        if (!isActive) {
            Logger.warn("Batch is immutable, can not append.");
            return;
        }
        if (this.currentSizeInBytes + record.length > segmentThresholdInBytes) {
            Logger.warn("Size of record exceeds log segment threshold.");
            return;
        }

        // store entry before modifying bytes first
        this.entries.createNewEntry(currRecordOffset, currentSizeInBytes);
        appendDataToLogFile(record);
    }

    public Message getRecordFromSegmentAtOffset(int recordOffset) throws IOException {
        System.out.println("Offsets contents: " + entries.recordOffsetToByteOffsets.entrySet());
        if (!entries.recordOffsetToByteOffsets.containsKey(recordOffset)) {
            System.out.println("Key = " + recordOffset);
            System.err.println("Record not found: " + recordOffset);
            return null;
        }
        if (recordOffset > this.endOffset) {
            System.out.println("No more records to read");
            return null;
        }

        int byteOffset = entries.recordOffsetToByteOffsets.get(recordOffset);
        System.out.println("RECORD OFFSET = " + recordOffset + " | CORRES. BYTE OFFSET = " + byteOffset);
        // !!! NOTE: RETURNS EMPTY IF THE LOG FILES ARE NOT YET CREATED. HANDLE THIS LATER
        try (RandomAccessFile raf = new RandomAccessFile(logFile.getPath(), "r")) {
            System.out.println("BYTE OFFSET: " + byteOffset);
            raf.seek(byteOffset);

            byte[] header = new byte[12];
            int bytesRead = raf.read(header);
            System.out.println("HEADER: " + Arrays.toString(header));

            ByteBuffer buffer = ByteBuffer.wrap(header);
            int recordSize = buffer.getInt(8);

            System.out.println("RECORD SIZE: " + recordSize);

            raf.seek(byteOffset);
            byte[] data = new byte[recordSize + 12];
            raf.read(data);

            System.out.println("PRINTING DATA: " + Arrays.toString(data));

            // deserialize into producer rec first so we can get the fields
            ProducerRecord<String, String> rec = ProducerRecordCodec.deserialize(
                    data,
                    String.class,
                    String.class
            );

            Message msg = Message
                    .newBuilder()
                    .setTopic(rec.getTopic())
                    .setPartition(rec.getPartitionNumber() == null ? 1 : rec.getPartitionNumber())
                    .setOffset(recordOffset)
                    .setTimestamp(rec.getTimestamp())
                    .setKey(rec.getKey() == null ? "" : rec.getKey())
                    .setValue(rec.getValue()).buildPartial();

            System.out.println("\nPRINTING DESERIALIZED PRODREC---------- \n" + rec + "\n");
            return msg;
        }
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

        appendDataToLogFile(occupiedData);
    }

    private void appendDataToLogFile(byte[] data) throws IOException {
        try {
            prependByteOffset(data);

            // write occupied data to the log file
            Files.write(Path.of(logFile.getPath()), data, StandardOpenOption.APPEND);

            System.out.println("-----------------------------------------------PRINTING ALL BYTE DATA IN FILE-----------------------------------------------");
            byte[] testData = Files.readAllBytes(Path.of(logFile.getPath()));
            System.out.println(Arrays.toString(testData));
            System.out.println("-----------------------------------------------END OF PRINTING ALL BYTE DATA IN FILE-----------------------------------------------");

            this.currentSizeInBytes += data.length;
            this.endOffset++;
            Logger.info("3. Data successfully written to segment.");
        } catch (IOException e) {
            Logger.error("Failed to write data to log segment.", e);
            throw e;
        }
    }

    private void prependByteOffset(byte[] data) {
        // first 4 bytes are for the record offset, next 4 for byte offset
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(4, currentSizeInBytes);
    }

    public boolean isActive() {
        return isActive;
    }

    public File getLogFile() {
        return logFile;
    }

    public File getIndexFile() {
        return indexFile;
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
                ", \nendOffset=" + getEndOffset() +
                '}';
    }
}
