package commons.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;

public class CompressionUtilTest {

    @Test
    @DisplayName("Test basic GZIP compression and decompression")
    public void testBasicCompressionDecompression() throws IOException {
        String testData = "This is a test string that should compress well when repeated. ".repeat(10);
        byte[] originalData = testData.getBytes();

        // Compress the data
        byte[] compressedData = CompressionUtil.gzipCompress(originalData);

        // Verify compression occurred (should be smaller)
        assertTrue(compressedData.length < originalData.length, 
                  "Compressed data should be smaller than original");

        // Decompress the data
        byte[] decompressedData = CompressionUtil.gzipDecompress(compressedData);

        // Verify the round trip
        assertArrayEquals(originalData, decompressedData, 
                         "Decompressed data should match original");
    }

    @Test
    @DisplayName("Test compression with empty data")
    public void testCompressionWithEmptyData() throws IOException {
        byte[] emptyData = new byte[0];

        byte[] compressedEmpty = CompressionUtil.gzipCompress(emptyData);
        byte[] decompressedEmpty = CompressionUtil.gzipDecompress(compressedEmpty);

        assertArrayEquals(emptyData, compressedEmpty, "Empty data should remain empty after compression");
        assertArrayEquals(emptyData, decompressedEmpty, "Empty data should remain empty after decompression");
    }

    @Test
    @DisplayName("Test compression with null data")
    public void testCompressionWithNullData() throws IOException {
        byte[] nullData = null;

        byte[] compressedNull = CompressionUtil.gzipCompress(nullData);
        byte[] decompressedNull = CompressionUtil.gzipDecompress(nullData);

        assertNull(compressedNull, "Null data should remain null after compression");
        assertNull(decompressedNull, "Null data should remain null after decompression");
    }

    @Test
    @DisplayName("Test compression beneficial check")
    public void testCompressionBeneficial() {
        // Test with highly compressible data
        String repetitiveData = "A".repeat(1000);
        byte[] originalData = repetitiveData.getBytes();
        byte[] smallCompressedData = new byte[100]; // Simulated compressed data

        assertTrue(CompressionUtil.isCompressionBeneficial(originalData, smallCompressedData),
                  "Should consider compression beneficial when size is significantly reduced");

        // Test with data that doesn't compress well
        byte[] largeCompressedData = new byte[950]; // Only 5% reduction
        assertFalse(CompressionUtil.isCompressionBeneficial(originalData, largeCompressedData),
                   "Should not consider compression beneficial when reduction is less than 5%");

        // Test with compressed data larger than original
        byte[] largerCompressedData = new byte[1100];
        assertFalse(CompressionUtil.isCompressionBeneficial(originalData, largerCompressedData),
                   "Should not consider compression beneficial when compressed data is larger");
    }

    @Test
    @DisplayName("Test compression with real producer record data")
    public void testCompressionWithProducerRecordData() throws IOException {
        // Simulate serialized producer record data with repetitive content
        String jsonLikeData = "{\"key\":\"user123\",\"value\":\"This is message content that repeats often\",\"timestamp\":1234567890}";
        String repeatedData = jsonLikeData.repeat(20); // Simulate batch of similar records
        byte[] originalData = repeatedData.getBytes();

        byte[] compressedData = CompressionUtil.gzipCompress(originalData);
        byte[] decompressedData = CompressionUtil.gzipDecompress(compressedData);

        // Verify compression works
        assertTrue(compressedData.length < originalData.length,
                  "Producer record data should compress well");
        assertArrayEquals(originalData, decompressedData,
                         "Round trip should preserve data integrity");

        // Verify compression is beneficial
        assertTrue(CompressionUtil.isCompressionBeneficial(originalData, compressedData),
                  "Compression should be beneficial for repetitive producer data");
    }

    @Test
    @DisplayName("Test compression with random data")
    public void testCompressionWithRandomData() throws IOException {
        // Random data typically doesn't compress well
        byte[] randomData = new byte[1000];
        for (int i = 0; i < randomData.length; i++) {
            randomData[i] = (byte) (Math.random() * 256);
        }

        byte[] compressedData = CompressionUtil.gzipCompress(randomData);
        byte[] decompressedData = CompressionUtil.gzipDecompress(compressedData);

        // Verify data integrity even if compression isn't beneficial
        assertArrayEquals(randomData, decompressedData,
                         "Random data should maintain integrity through compression round trip");

        // Random data may or may not compress well, but should not cause errors
        assertNotNull(compressedData, "Compression should handle random data without errors");
    }
}