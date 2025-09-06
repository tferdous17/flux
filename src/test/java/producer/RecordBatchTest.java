package producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

public class RecordBatchTest {

    @Test
    public void recordBatchDefaultConstructorTest() {
        RecordBatch batch = new RecordBatch();
        batch.printBatchDetails(); // should print 10240 bytes as max batch size
    }

    @Test
    public void recordBatchParamConstructorTest() {
        RecordBatch batch = new RecordBatch(500_000);
        batch.printBatchDetails(); // should print 500,000 as max batch size
    }

    @Test
    public void addSerializedRecordToBatchTest() throws IOException {
        RecordBatch batch = new RecordBatch();
        byte[] record = {10, 39, 122, 19, 93, 34, 9};
        boolean res = batch.append(record);

        if (res) {
            System.out.println("record successfully added\n");
        } else {
            System.out.println("record NOT successfully added");
        }
    }

    @Test
    public void testCreationTime() {
        long beforeCreation = System.currentTimeMillis();
        RecordBatch batch = new RecordBatch();
        long afterCreation = System.currentTimeMillis();
        
        assertTrue(batch.getCreationTime() >= beforeCreation);
        assertTrue(batch.getCreationTime() <= afterCreation);
    }

    @Test
    public void testIsFull() throws IOException {
        RecordBatch batch = new RecordBatch(100); // Small batch size
        assertFalse(batch.isFull());
        
        // Add a large record to fill the batch
        byte[] largeRecord = new byte[50];
        batch.append(largeRecord);
        assertFalse(batch.isFull());
        
        // Add another record that would exceed the limit
        byte[] anotherRecord = new byte[60];
        assertFalse(batch.append(anotherRecord)); // Should return false (won't fit)
        assertFalse(batch.isFull()); // Still not full since append failed
    }

    @Test
    public void testGetAge() throws InterruptedException {
        RecordBatch batch = new RecordBatch();
        long initialAge = batch.getAge();
        assertTrue(initialAge >= 0);
        
        // Wait a bit and check age increased
        Thread.sleep(10);
        long laterAge = batch.getAge();
        assertTrue(laterAge > initialAge);
    }

    @Test
    public void testGetters() throws IOException {
        RecordBatch batch = new RecordBatch(1000);
        assertEquals(1000, batch.getMaxBatchSizeInBytes());
        assertEquals(0, batch.getCurrBatchSizeInBytes());
        assertEquals(0, batch.getRecordCount());
        
        byte[] record = new byte[100];
        batch.append(record);
        
        assertEquals(100, batch.getCurrBatchSizeInBytes());
        assertEquals(1, batch.getRecordCount());
    }

    @Test
    @DisplayName("Test batch compression with compressible data")
    public void testBatchCompressionWithCompressibleData() throws IOException {
        RecordBatch batch = new RecordBatch(10000);
        
        // Add repetitive data that should compress well
        String repetitiveContent = "This is a test record with repetitive content. ".repeat(10);
        byte[] record1 = repetitiveContent.getBytes();
        byte[] record2 = repetitiveContent.getBytes();
        byte[] record3 = repetitiveContent.getBytes();
        
        batch.append(record1);
        batch.append(record2);
        batch.append(record3);
        
        assertFalse(batch.isCompressed());
        
        // Compress the batch
        boolean compressionApplied = batch.compress();
        assertTrue(compressionApplied, "Compression should be applied to repetitive data");
        assertTrue(batch.isCompressed());
        
        // Verify compression reduced size
        assertTrue(batch.getDataSize() < batch.getCurrBatchSizeInBytes(),
                  "Compressed size should be smaller than original");
        
        // Verify we can get the compressed data
        byte[] compressedData = batch.getData();
        assertNotNull(compressedData);
        assertEquals(batch.getDataSize(), compressedData.length);
    }

    @Test
    @DisplayName("Test batch compression with non-compressible data")
    public void testBatchCompressionWithNonCompressibleData() throws IOException {
        RecordBatch batch = new RecordBatch(1000);
        
        // Add random data that won't compress well
        byte[] randomRecord = new byte[100];
        for (int i = 0; i < randomRecord.length; i++) {
            randomRecord[i] = (byte) (Math.random() * 256);
        }
        
        batch.append(randomRecord);
        
        // Try to compress
        boolean compressionApplied = batch.compress();
        
        // Compression may or may not be applied depending on the random data,
        // but it should not cause errors
        assertNotNull(batch.getData());
    }

    @Test
    @DisplayName("Test compression on empty batch")
    public void testCompressionOnEmptyBatch() throws IOException {
        RecordBatch batch = new RecordBatch();
        
        // Try to compress empty batch
        boolean compressionApplied = batch.compress();
        assertFalse(compressionApplied, "Empty batch should not be compressed");
        assertFalse(batch.isCompressed());
        
        // Should be able to get data from empty batch
        byte[] data = batch.getData();
        assertNotNull(data);
        assertEquals(0, data.length);
    }

    @Test
    @DisplayName("Test double compression")
    public void testDoubleCompression() throws IOException {
        RecordBatch batch = new RecordBatch(1000);
        String content = "Compressible content ".repeat(20);
        batch.append(content.getBytes());
        
        // First compression
        boolean firstCompression = batch.compress();
        boolean isCompressedAfterFirst = batch.isCompressed();
        
        // Second compression attempt
        boolean secondCompression = batch.compress();
        
        // Second compression should return the same result as the first
        assertEquals(isCompressedAfterFirst, secondCompression);
        assertEquals(isCompressedAfterFirst, batch.isCompressed());
    }

    @Test
    @DisplayName("Test getData method for both compressed and uncompressed batches")
    public void testGetDataMethod() throws IOException {
        RecordBatch batch = new RecordBatch(1000);
        String testContent = "Test content for getData method";
        byte[] originalData = testContent.getBytes();
        
        batch.append(originalData);
        
        // Get data before compression
        byte[] uncompressedData = batch.getData();
        assertEquals(originalData.length, uncompressedData.length);
        
        // Compress if beneficial
        batch.compress();
        
        // Get data after compression (may be compressed or original)
        byte[] dataAfterCompression = batch.getData();
        assertNotNull(dataAfterCompression);
        assertEquals(batch.getDataSize(), dataAfterCompression.length);
    }
}
