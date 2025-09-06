package commons.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtil {
    
    /**
     * Compress data using GZIP compression
     * @param data the data to compress
     * @return compressed data
     * @throws IOException if compression fails
     */
    public static byte[] gzipCompress(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return data;
        }
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(data);
        }
        return baos.toByteArray();
    }
    
    /**
     * Decompress GZIP compressed data
     * @param compressedData the compressed data
     * @return decompressed data
     * @throws IOException if decompression fails
     */
    public static byte[] gzipDecompress(byte[] compressedData) throws IOException {
        if (compressedData == null || compressedData.length == 0) {
            return compressedData;
        }
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(compressedData);
             GZIPInputStream gzipIn = new GZIPInputStream(bais)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIn.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
        }
        return baos.toByteArray();
    }
    
    /**
     * Check if compression is beneficial (reduces size)
     * @param originalData the original data
     * @param compressedData the compressed data
     * @return true if compression reduces size by at least 5%
     */
    public static boolean isCompressionBeneficial(byte[] originalData, byte[] compressedData) {
        if (originalData == null || compressedData == null) {
            return false;
        }
        // Only compress if it reduces size by at least 5%
        return compressedData.length < (originalData.length * 0.95);
    }
}