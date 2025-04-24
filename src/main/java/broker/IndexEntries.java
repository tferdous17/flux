package broker;

import commons.FluxExecutor;
import org.tinylog.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a Record Offset --> Byte Offset pair
 */
public class IndexEntries {
    public Map<Integer, Integer> recordOffsetToByteOffsets;
    private final int flushThreshold = 3; // note: contents will be empty after hitting this threshold
    private File indexFile;
    private int offsetPtr;

    public IndexEntries(File indexFile) {
        recordOffsetToByteOffsets = new HashMap<>();
        this.indexFile = indexFile;
        offsetPtr = 0;
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

        for (int i = offsetPtr; i < recordOffsetToByteOffsets.size(); i++) {
            buffer.putInt(i);
            buffer.putInt(recordOffsetToByteOffsets.get(i));
        }

        FluxExecutor.getExecutorService().submit(() -> {
            try {
                // note: BufferedOutputStream is a viable alternative here as well for Files.write()
                Files.write(Path.of(indexFile.getPath()), buffer.array(), StandardOpenOption.APPEND);

                Logger.info("\u001B[32m" + "Index entry flush completed." + "\u001B[0m");
                offsetPtr = recordOffsetToByteOffsets.size();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }
}
