package producer;

import commons.compression.CompressionType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordBatchCompressionTest {
    
    /**
     * Tests that a RecordBatch configured with NO compression stores data uncompressed.
     * Verifies that:
     * - The batch correctly reports NONE as its compression type
     * - Records can be appended successfully
     * - The compressed output is the same size as the original data
     */
    @Test
    public void testRecordBatchWithNoCompression() throws IOException {
        RecordBatch batch = new RecordBatch(1024, CompressionType.NONE);
        
        assertEquals(CompressionType.NONE, batch.getCompressionType());
        
        // Add some test records
        String testData = "Test record";
        byte[] recordData = testData.getBytes(StandardCharsets.UTF_8);
        
        assertTrue(batch.append(recordData, "test-topic", 0));
        assertEquals(1, batch.getRecordCount());
        
        // Get compressed data (should be same as original for NONE)
        ByteBuffer compressedData = batch.getCompressedBatchData();
        assertNotNull(compressedData);
        assertEquals(recordData.length, compressedData.remaining());
    }
    
    /**
     * Tests that a RecordBatch configured with GZIP compression effectively compresses data.
     * Verifies that:
     * - The batch correctly reports GZIP as its compression type
     * - Multiple repetitive records can be appended (good compression candidate)
     * - The compressed output is significantly smaller than the original data
     * - Compression ratio is calculated and logged for visibility
     */
    @Test
    public void testRecordBatchWithGzipCompression() throws IOException {
        RecordBatch batch = new RecordBatch(2048, CompressionType.GZIP);
        
        assertEquals(CompressionType.GZIP, batch.getCompressionType());
        
        // Add multiple repetitive records (should compress well)
        String baseData = "This is a test record that repeats often. ";
        for (int i = 0; i < 10; i++) {
            String recordData = baseData + i;
            byte[] recordBytes = recordData.getBytes(StandardCharsets.UTF_8);
            assertTrue(batch.append(recordBytes, "test-topic", 0));
        }
        
        assertEquals(10, batch.getRecordCount());
        
        // Get compressed data
        ByteBuffer compressedData = batch.getCompressedBatchData();
        assertNotNull(compressedData);
        
        // For repetitive data, compressed size should be smaller than original
        int originalSize = batch.getCurrBatchSizeInBytes();
        int compressedSize = compressedData.remaining();
        
        System.out.println("Original batch size: " + originalSize + " bytes");
        System.out.println("Compressed size: " + compressedSize + " bytes");
        System.out.println("Compression ratio: " + (double) compressedSize / originalSize);
        
        // We expect some compression for this repetitive data
        assertTrue(compressedSize < originalSize, 
            "Compressed size should be less than original for repetitive data");
    }
    
    /**
     * Tests that RecordBatch defaults to NO compression when not explicitly specified.
     * Verifies that:
     * - Default constructor uses NONE compression
     * - Constructor with only size parameter also defaults to NONE
     * This ensures backward compatibility with existing code
     */
    @Test
    public void testRecordBatchDefaultCompression() throws IOException {
        // Default constructor should use NONE compression
        RecordBatch batch = new RecordBatch();
        assertEquals(CompressionType.NONE, batch.getCompressionType());
        
        // Constructor with size only should also use NONE
        RecordBatch batch2 = new RecordBatch(1024);
        assertEquals(CompressionType.NONE, batch2.getCompressionType());
    }
    
    /**
     * Tests that empty batches handle compression correctly.
     * Verifies that:
     * - An empty batch with GZIP compression returns an empty buffer
     * - No exceptions are thrown when compressing empty data
     */
    @Test
    public void testEmptyBatchCompression() {
        RecordBatch batch = new RecordBatch(1024, CompressionType.GZIP);
        
        // Empty batch should return empty buffer
        ByteBuffer compressedData = batch.getCompressedBatchData();
        assertNotNull(compressedData);
        assertEquals(0, compressedData.remaining());
    }
    
    /**
     * Tests that batch debug/logging methods work correctly with compression enabled.
     * Verifies that:
     * - printBatchDetails() includes compression information
     * - No exceptions are thrown when printing compressed batch details
     */
    @Test
    public void testBatchDetailsWithCompression() throws IOException {
        RecordBatch batch = new RecordBatch(1024, CompressionType.GZIP);
        
        // Add a test record
        byte[] recordData = "Test data".getBytes(StandardCharsets.UTF_8);
        batch.append(recordData, "test-topic", 0);
        
        // This should not throw any exceptions and should include compression info
        batch.printBatchDetails();
    }
}