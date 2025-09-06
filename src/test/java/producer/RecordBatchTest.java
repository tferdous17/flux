package producer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

public class RecordBatchTest {

    @Test
    public void testBasicOperations() throws IOException {
        RecordBatch batch = new RecordBatch(1000);
        assertEquals(1000, batch.getMaxBatchSizeInBytes());
        assertEquals(0, batch.getCurrBatchSizeInBytes());
        assertEquals(0, batch.getRecordCount());

        byte[] record = new byte[100];
        assertTrue(batch.append(record));
        assertEquals(100, batch.getCurrBatchSizeInBytes());
        assertEquals(1, batch.getRecordCount());

        // Test batch full
        byte[] largeRecord = new byte[950];
        assertFalse(batch.append(largeRecord));
    }

    @Test
    public void testTimingAndAge() throws InterruptedException {
        long before = System.currentTimeMillis();
        RecordBatch batch = new RecordBatch();
        long after = System.currentTimeMillis();

        assertTrue(batch.getCreationTime() >= before);
        assertTrue(batch.getCreationTime() <= after);

        Thread.sleep(10);
        assertTrue(batch.getAge() >= 10);
    }

    @Test
    public void testCompression() throws IOException {
        RecordBatch batch = new RecordBatch(10000);

        // Add repetitive data
        String repetitive = "test ".repeat(100);
        batch.append(repetitive.getBytes());
        batch.append(repetitive.getBytes());

        assertFalse(batch.isCompressed());
        int originalSize = batch.getCurrBatchSizeInBytes();

        // Compress
        assertTrue(batch.compress());
        assertTrue(batch.isCompressed());
        assertTrue(batch.getDataSize() < originalSize);

        // Verify compressed data
        byte[] compressed = batch.getData();
        assertNotNull(compressed);
        assertEquals(batch.getDataSize(), compressed.length);
    }

    @Test
    public void testEmptyBatchCompression() throws IOException {
        RecordBatch batch = new RecordBatch();
        assertFalse(batch.compress());
        assertFalse(batch.isCompressed());
        assertEquals(0, batch.getData().length);
    }

    @Test
    public void testDoubleCompression() throws IOException {
        RecordBatch batch = new RecordBatch(1000);
        batch.append("compressible ".repeat(20).getBytes());

        boolean first = batch.compress();
        boolean isCompressed = batch.isCompressed();
        boolean second = batch.compress();

        assertEquals(isCompressed, second);
        assertEquals(isCompressed, batch.isCompressed());
    }
}