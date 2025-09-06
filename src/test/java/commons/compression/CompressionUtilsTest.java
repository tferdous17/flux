package commons.compression;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CompressionUtilsTest {
    
    @Test
    public void testGzipCompressionRoundTrip() throws IOException {
        // Test data that should compress well
        String testData = "This is a test string that repeats. ".repeat(10);
        byte[] originalBytes = testData.getBytes(StandardCharsets.UTF_8);
        ByteBuffer originalBuffer = ByteBuffer.wrap(originalBytes);
        
        // Compress
        ByteBuffer compressedBuffer = CompressionUtils.compress(originalBuffer, CompressionType.GZIP);
        assertNotNull(compressedBuffer);
        assertTrue(compressedBuffer.remaining() < originalBytes.length, 
            "Compressed data should be smaller than original");
        
        // Decompress
        ByteBuffer decompressedBuffer = CompressionUtils.decompress(compressedBuffer, CompressionType.GZIP);
        assertNotNull(decompressedBuffer);
        assertEquals(originalBytes.length, decompressedBuffer.remaining());
        
        // Verify data integrity
        byte[] decompressedBytes = new byte[decompressedBuffer.remaining()];
        decompressedBuffer.get(decompressedBytes);
        String decompressedData = new String(decompressedBytes, StandardCharsets.UTF_8);
        assertEquals(testData, decompressedData);
        
        System.out.println("Original size: " + originalBytes.length);
        System.out.println("Compressed size: " + compressedBuffer.capacity());
        System.out.println("Compression ratio: " + 
            (double) compressedBuffer.capacity() / originalBytes.length);
    }
    
    @Test
    public void testNoCompressionPassthrough() throws IOException {
        String testData = "Test data for no compression";
        byte[] originalBytes = testData.getBytes(StandardCharsets.UTF_8);
        ByteBuffer originalBuffer = ByteBuffer.wrap(originalBytes);
        
        // Test compression with NONE
        ByteBuffer result = CompressionUtils.compress(originalBuffer, CompressionType.NONE);
        assertEquals(originalBuffer, result, "NONE compression should return original buffer");
        
        // Test decompression with NONE  
        ByteBuffer decompressed = CompressionUtils.decompress(originalBuffer, CompressionType.NONE);
        assertEquals(originalBuffer, decompressed, "NONE decompression should return original buffer");
    }
    
    @Test
    public void testEmptyDataCompression() throws IOException {
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        
        // Compress empty data
        ByteBuffer compressed = CompressionUtils.compress(emptyBuffer, CompressionType.GZIP);
        assertNotNull(compressed);
        
        // Decompress should work without error
        ByteBuffer decompressed = CompressionUtils.decompress(compressed, CompressionType.GZIP);
        assertNotNull(decompressed);
    }
    
    @Test
    public void testNullDataHandling() throws IOException {
        // Test null handling
        ByteBuffer result = CompressionUtils.compress(null, CompressionType.GZIP);
        assertNull(result);
        
        ByteBuffer decompressed = CompressionUtils.decompress(null, CompressionType.GZIP);
        assertNull(decompressed);
    }
}