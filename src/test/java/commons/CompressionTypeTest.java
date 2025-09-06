package commons;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

public class CompressionTypeTest {

    @Test
    public void testAllCompressionTypes() throws IOException {
        String testData = "This is a test string that repeats. ".repeat(10);
        byte[] original = testData.getBytes();

        for (CompressionType type : CompressionType.values()) {
            byte[] compressed = type.compress(original);
            byte[] decompressed = type.decompress(compressed);
            
            assertArrayEquals(original, decompressed, 
                "Failed for compression type: " + type.getName());
            
            if (type != CompressionType.NONE) {
                // Most compression types should reduce size for repetitive data
                assertTrue(compressed.length <= original.length,
                    "Compression type " + type.getName() + " should reduce or maintain size");
            } else {
                // NONE should return same data
                assertArrayEquals(original, compressed,
                    "NONE compression should return original data");
            }
        }
    }

    @Test
    public void testEmptyData() throws IOException {
        byte[] empty = new byte[0];
        
        for (CompressionType type : CompressionType.values()) {
            byte[] compressed = type.compress(empty);
            byte[] decompressed = type.decompress(compressed);
            
            assertEquals(0, decompressed.length,
                "Empty data should remain empty for " + type.getName());
        }
    }

    @Test
    public void testNullData() throws IOException {
        for (CompressionType type : CompressionType.values()) {
            byte[] compressed = type.compress(null);
            byte[] decompressed = type.decompress(null);
            
            assertNull(compressed, "Null should return null for " + type.getName());
            assertNull(decompressed, "Null should return null for " + type.getName());
        }
    }

    @Test
    public void testCompressionEfficiency() throws IOException {
        // Highly compressible data
        String repetitive = "A".repeat(1000);
        byte[] original = repetitive.getBytes();
        
        // Test each compression type
        byte[] gzipCompressed = CompressionType.GZIP.compress(original);
        byte[] snappyCompressed = CompressionType.SNAPPY.compress(original);
        byte[] lz4Compressed = CompressionType.LZ4.compress(original);
        byte[] zstdCompressed = CompressionType.ZSTD.compress(original);
        
        // All should significantly compress repetitive data
        assertTrue(gzipCompressed.length < original.length / 10);
        assertTrue(snappyCompressed.length < original.length / 10);
        assertTrue(lz4Compressed.length < original.length / 10);
        assertTrue(zstdCompressed.length < original.length / 10);
        
        // Verify decompression
        assertArrayEquals(original, CompressionType.GZIP.decompress(gzipCompressed));
        assertArrayEquals(original, CompressionType.SNAPPY.decompress(snappyCompressed));
        assertArrayEquals(original, CompressionType.LZ4.decompress(lz4Compressed));
        assertArrayEquals(original, CompressionType.ZSTD.decompress(zstdCompressed));
    }

    @Test
    public void testFromId() {
        assertEquals(CompressionType.NONE, CompressionType.fromId(0));
        assertEquals(CompressionType.GZIP, CompressionType.fromId(1));
        assertEquals(CompressionType.SNAPPY, CompressionType.fromId(2));
        assertEquals(CompressionType.LZ4, CompressionType.fromId(3));
        assertEquals(CompressionType.ZSTD, CompressionType.fromId(4));
        
        assertThrows(IllegalArgumentException.class, () -> CompressionType.fromId(99));
    }

    @Test
    public void testFromName() {
        assertEquals(CompressionType.NONE, CompressionType.fromName("none"));
        assertEquals(CompressionType.GZIP, CompressionType.fromName("gzip"));
        assertEquals(CompressionType.SNAPPY, CompressionType.fromName("snappy"));
        assertEquals(CompressionType.LZ4, CompressionType.fromName("lz4"));
        assertEquals(CompressionType.ZSTD, CompressionType.fromName("zstd"));
        
        // Case insensitive
        assertEquals(CompressionType.GZIP, CompressionType.fromName("GZIP"));
        assertEquals(CompressionType.SNAPPY, CompressionType.fromName("Snappy"));
        
        assertThrows(IllegalArgumentException.class, () -> CompressionType.fromName("invalid"));
    }

    @Test
    public void testLargeData() throws IOException {
        // Test with larger realistic data
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("{\"id\":").append(i)
              .append(",\"name\":\"user").append(i)
              .append("\",\"timestamp\":").append(System.currentTimeMillis())
              .append(",\"data\":\"some payload data\"}\n");
        }
        byte[] original = sb.toString().getBytes();
        
        for (CompressionType type : CompressionType.values()) {
            if (type == CompressionType.NONE) continue;
            
            byte[] compressed = type.compress(original);
            byte[] decompressed = type.decompress(compressed);
            
            assertArrayEquals(original, decompressed,
                "Large data test failed for " + type.getName());
            
            // JSON should compress well
            assertTrue(compressed.length < original.length * 0.5,
                type.getName() + " should achieve good compression for JSON data");
        }
    }
}