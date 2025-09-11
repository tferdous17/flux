package commons.utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

public class CompressionUtilTest {

    @Test
    public void testBasicCompressionDecompression() throws IOException {
        String testData = "This is a test string that repeats. ".repeat(10);
        byte[] original = testData.getBytes();

        byte[] compressed = CompressionUtil.gzipCompress(original);
        assertTrue(compressed.length < original.length);

        byte[] decompressed = CompressionUtil.gzipDecompress(compressed);
        assertArrayEquals(original, decompressed);
    }

    @Test
    public void testEdgeCases() throws IOException {
        // Empty data
        byte[] empty = new byte[0];
        byte[] compressedEmpty = CompressionUtil.gzipCompress(empty);
        byte[] decompressedEmpty = CompressionUtil.gzipDecompress(compressedEmpty);
        assertArrayEquals(empty, compressedEmpty);
        assertArrayEquals(empty, decompressedEmpty);

        // Null data
        assertNull(CompressionUtil.gzipCompress(null));
        assertNull(CompressionUtil.gzipDecompress(null));
    }


    @Test
    public void testWithProducerRecordData() throws IOException {
        // Simulate batch of similar records
        String record = "{\"key\":\"user123\",\"value\":\"message\",\"timestamp\":1234567890}";
        byte[] original = record.repeat(20).getBytes();

        byte[] compressed = CompressionUtil.gzipCompress(original);
        byte[] decompressed = CompressionUtil.gzipDecompress(compressed);

        assertTrue(compressed.length < original.length);
        assertArrayEquals(original, decompressed);
    }

    @Test
    public void testWithRandomData() throws IOException {
        // Random data doesn't compress well
        byte[] random = new byte[1000];
        for (int i = 0; i < random.length; i++) {
            random[i] = (byte) (Math.random() * 256);
        }

        byte[] compressed = CompressionUtil.gzipCompress(random);
        byte[] decompressed = CompressionUtil.gzipDecompress(compressed);

        assertArrayEquals(random, decompressed);
        assertNotNull(compressed);
    }
}