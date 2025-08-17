package server.internal.storage;

import commons.FluxExecutor;
import org.tinylog.Logger;
import producer.ProducerRecord;
import producer.ProducerRecordCodec;
import producer.RecordBatch;
import proto.Message;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

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
    
    private final int FLUSH_THRESHOLD_IN_BYTES = 524_288; // 1:5 ratio wrt segment threshold, ratio chosen for high throughput
    private ByteBuffer buffer = ByteBuffer.allocateDirect(FLUSH_THRESHOLD_IN_BYTES);
    // record offset -> (byte offset, record length)
    private Map<Integer, int[]> recOffsetToByteOffsetAndRecLen = new HashMap<>();
    private Accumulator accumulator;

    // temporarily store info in line w/ buffer to be committed after flush
    // note: this is for index entries specifically, so it only gets populated when data is written to disk
    private class Accumulator {
        public int bytes;
        public int numRecords;
        Map<Integer, Integer> tempRecToByteOffsets;

        public Accumulator() {
            bytes = 0;
            numRecords = 0;
            tempRecToByteOffsets = new HashMap<>();
        }

        void reset() {
            bytes = 0;
            numRecords = 0;
            tempRecToByteOffsets.clear();
        }
    }

    public LogSegment(String topic, int partitionNumber, int startOffset) throws IOException {
        this.partitionNumber = partitionNumber;
        this.startOffset = startOffset;

        try {
            createDataFolder();

            // ex: my-topic_partition1_000000.log, actual kafka names log files purely by byte offset like 000000123401.log
            String logFileName = String.format("data/%s_partition%d_%05d.log", topic, this.partitionNumber, this.startOffset);
            String indexFileName = String.format("data/%s_partition%d_%05d.index", topic, this.partitionNumber, this.startOffset);

            this.logFile = createFile(logFileName);
            this.indexFile = createFile(indexFileName);
            entries = new IndexEntries(indexFile);

        } catch (IOException e) {
            Logger.error("IOException occurred while creating LogSegment files.");
            throw e;
        }

        this.isActive = true; // by default, this LogSegment is active upon creation, but tread carefully
        this.currentSizeInBytes = 0;
        this.accumulator = new Accumulator();
    }

    // overloaded constructor in case we want to manually define threshold
    public LogSegment(String topic, int partitionNumber, int startOffset, int segmentThresholdInBytes) throws IOException {
        this(topic, partitionNumber, startOffset);
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

    public void appendToBuffer(byte[] data, int currRecordOffset) {
        if (data == null || data.length == 0) {
            Logger.warn("Data null or empty");
            return;
        }

        // not enough space for data or exceeds threshold
        if (buffer.remaining() < data.length || buffer.position() > FLUSH_THRESHOLD_IN_BYTES) {
            flushAsync();
        }

        if (data.length > FLUSH_THRESHOLD_IN_BYTES) {
            // obnoxiously large record
            Logger.warn("Message length too large. Cannot append");
            return;
        }
        System.out.println("BUFFER POSITION: " + buffer.position());
        recOffsetToByteOffsetAndRecLen.put(currRecordOffset, new int[]{buffer.position(), data.length});
        buffer.put(data);
        System.out.println("DATA AFTER BUFFER PUT IN: " + Arrays.toString(data) + " its length = " + data.length);

        // put in temp rec to byte offsets
        accumulator.tempRecToByteOffsets.put(currRecordOffset, currentSizeInBytes);
        currentSizeInBytes += data.length;
        accumulator.bytes += data.length;
        accumulator.numRecords++;

        System.out.println("CURRENT SiZE IN BYTES: " + currentSizeInBytes);
    }

    private void flushAsync() {
        if (buffer.position() == 0) {
            return;
        }
        Logger.info("**************--------------------Commencing flushing process | Threshold = %d, Buffer = %d", FLUSH_THRESHOLD_IN_BYTES, buffer.flip().remaining());
        buffer.flip(); // important to set position pointer to 0
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        // should i submit copy of the data to be flushed or original? come back later
        Future<?> future = FluxExecutor.getExecutorService().submit(() -> {
            try {
                // note: BufferedOutputStream is a viable alternative here as well for Files.write()
                Files.write(Path.of(logFile.getPath()), data, StandardOpenOption.APPEND);

                // after data written to disk, then commit entries and offsets and all dat
                this.currentSizeInBytes += accumulator.bytes;
                this.endOffset += accumulator.numRecords;
                accumulator.tempRecToByteOffsets.forEach((key, value) -> {
                    if (!entries.recordOffsetToByteOffsets.containsKey(key)) {
                        entries.createNewEntry(key, value);
                    }
                });
                accumulator.reset();
                helperMethodToPrint();
                Logger.info("\u001B[32m" + "Flush completed and data written to disk." + "\u001B[0m");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        // clear buffer for further operations
        buffer = ByteBuffer.allocateDirect(FLUSH_THRESHOLD_IN_BYTES);
        recOffsetToByteOffsetAndRecLen.clear();

        FluxExecutor.getExecutorService().submit(() -> {
            try {
                entries.flushIndexEntries(); // write index entries to disk right after log file is written to
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // can optionally use future.get() and print lines for debugging to verify the file contents are written
        // (but it blocks main thread---only use for debugging)
    }

    // Return value dictates whether or not the segment is actually mutable or not
    public boolean writeRecordToSegment(byte[] record, int currRecordOffset) throws IOException {
        if (shouldBeImmutable()) {
            Logger.info("Log segment is now at capacity, setting as immutable.");
            setAsImmutable();
            return false;
        }
        if (!isActive) {
            Logger.warn("Batch is immutable, can not append.");
            return false;
        }
        if (this.currentSizeInBytes + record.length > segmentThresholdInBytes) {
            Logger.warn("Size of record exceeds log segment threshold.");
            return false;
        }

        prependByteOffset(record);
        appendToBuffer(record, currRecordOffset);
        return true;
    }

    public Message getRecordFromBuffer(int recordOffset) {
        Logger.info("Getting record with offset = " + recordOffset + " from buffer");
        int[] byteOffsetAndRecordLen = recOffsetToByteOffsetAndRecLen.get(recordOffset);
        Logger.info("Byte Offset and Record Len: " + Arrays.toString(byteOffsetAndRecordLen));
        byte[] data = new byte[byteOffsetAndRecordLen[1]];

        // TODO: Fix - At some point in testing (ConsumerServiceTest) this threw a `java.lang.IllegalArgumentException: newPosition > limit: (94 > 0)`
        // Might be because of an empty buffer or mismatch of values. Maybe byte offset didn't get reset properly when the buffer was flushed?
        // Error does not occur on every run of the test.
        buffer.position(byteOffsetAndRecordLen[0]);
        // issue is here
        buffer.get(data, 0, byteOffsetAndRecordLen[1]);
        System.out.println("DATA: " + Arrays.toString(data));

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

        System.out.println(msg);

        // reset buffer position to the end so that new records can get appended later w/o overwriting anything
        buffer.position(buffer.remaining());
        return msg;
    }

    public Message getRecordFromSegmentAtOffset(int recordOffset) throws IOException {
        // For records that are still in the internal buffer and not yet written to disk, they can still be fetched in the meantime
        if (recOffsetToByteOffsetAndRecLen.containsKey(recordOffset)) {
            Logger.info("Record to consume is still in buffer. Fetching from internal buffer.");
            return getRecordFromBuffer(recordOffset);
        }

        System.out.println("Offsets contents: " + entries.recordOffsetToByteOffsets.entrySet());
        if (!entries.recordOffsetToByteOffsets.containsKey(recordOffset)) {
            Logger.warn("Record not found with key = " + recordOffset);
            return null;
        }
        if (recordOffset > this.endOffset) {
            System.out.println("No more records to read.");
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
            if (bytesRead == -1) { // nothing to read, avoid error
                return null;
            }
            System.out.println("HEADER: " + Arrays.toString(header) + "\n | BYTES READ: " + bytesRead);

            ByteBuffer buffer = ByteBuffer.wrap(header);
            int recordSize = buffer.getInt(8);

            System.out.println("RECORD SIZE: " + recordSize);

            raf.seek(byteOffset);
            byte[] data = new byte[recordSize + 12];
            raf.read(data);

            helperMethodToPrint();

            // deserialize into producer rec first so we can get the fields
            ProducerRecord<String, String> rec = ProducerRecordCodec.deserialize(
                    data,
                    String.class,
                    String.class
            );

            Message msg = Message
                    .newBuilder()
                    .setTopic(rec.getTopic())
                    .setPartition(rec.getPartitionNumber() == null ? 1 : partitionNumber)
                    .setOffset(recordOffset)
                    .setTimestamp(rec.getTimestamp())
                    .setKey(rec.getKey() == null ? "" : rec.getKey())
                    .setValue(rec.getValue()).buildPartial();

            System.out.println("\nPRINTING DESERIALIZED PRODREC---------- \n" + rec + "\n");
            return msg;
        }
    }

    private void helperMethodToPrint() {
        try {
            byte[] bytes = Files.readAllBytes(Path.of(logFile.getPath()));
            System.out.println("ALL DATA: " + Arrays.toString(bytes) + " | LEN = " + bytes.length);
        } catch (Exception e) {
            e.printStackTrace();
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

//        appendDataToLogBuffer(occupiedData);
    }


    // NOTE: THIS MODIFIES THE DATA IN-PLACE
    private void prependByteOffset(byte[] data) {
        // first 4 bytes are for the record offset, next 4 for byte offset
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(4, currentSizeInBytes);
    }

    public boolean shouldBeImmutable() {
        return this.currentSizeInBytes >= segmentThresholdInBytes;
    }

    // once its immutable, the LogSegment can not be written to anymore (important)
    public void setAsImmutable() {
        this.isActive = false;
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
