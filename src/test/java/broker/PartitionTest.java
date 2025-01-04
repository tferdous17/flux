package broker;

import org.junit.jupiter.api.Test;
import producer.RecordBatch;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

// TODO: Fix failing test cases due to missing file, "./data/partition%d_%05d.log"
class PartitionTest {

    @Test
    void testAppendRecordBatch() throws IOException {
        Log log = new Log();
        Partition partition = new Partition(log, 0);
        LogSegment segment = new LogSegment(0, 0);

        log.getAllLogSegments().add(segment);

        RecordBatch batch = new RecordBatch();
        byte[] record1 = new byte[100];
        byte[] record2 = new byte[200];
        batch.append(record1);
        batch.append(record2);

        int startOffset = partition.appendRecordBatch(batch);

        assertEquals(0, startOffset);

        LogSegment activeSegment = log.getCurrentActiveLogSegment();
        assertTrue(activeSegment.getCurrentSizeInBytes() > 0);
        assertTrue(activeSegment.getEndOffset() > 0);
    }

    @Test
    void testAppendEmptyRecordBatch() throws IOException {
        Log log = new Log();
        Partition partition = new Partition(log, 0);

        assertThrows(IllegalArgumentException.class, () -> {
            RecordBatch emptyBatch = new RecordBatch();
            partition.appendRecordBatch(emptyBatch);
        });
    }

    @Test
    void testGetCurrentOffset() throws IOException {
        Log log = new Log();
        Partition partition = new Partition(log, 0);

        int currentOffset = partition.getCurrentOffset();
        assertEquals(log.getLogEndOffset(), currentOffset);

        RecordBatch batch = new RecordBatch();
        byte[] record = new byte[100];
        batch.append(record);
        partition.appendRecordBatch(batch);

        assertTrue(partition.getCurrentOffset() > currentOffset);
    }

    @Test
    void testGetPartitionId() throws IOException {
        Log log = new Log();
        Partition partition = new Partition(log, 0);

        assertEquals(0, partition.getPartitionId());
    }

    @Test
    void testGetLog() throws IOException {
        Log log = new Log();
        Partition partition = new Partition(log, 0);

        assertSame(log, partition.getLog());
    }
}
