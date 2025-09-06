package commons.compression;

import org.tinylog.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Simple compression utilities following Kafka's approach.
 * Compresses entire batch payloads, not individual records.
 */
public class CompressionUtils {
    
    /**
     * Compress a ByteBuffer using the specified compression type.
     * Returns the original buffer if compression is NONE or fails.
     * 
     * @param data The data to compress
     * @param compressionType The compression type to use
     * @return Compressed data as ByteBuffer
     */
    public static ByteBuffer compress(ByteBuffer data, CompressionType compressionType) {
        if (data == null || compressionType == CompressionType.NONE) {
            return data;
        }
        
        try {
            switch (compressionType) {
                case GZIP:
                    return compressGZIP(data);
                default:
                    Logger.warn("Unsupported compression type: {}, using uncompressed", compressionType);
                    return data;
            }
        } catch (IOException e) {
            Logger.warn("Compression failed with {}, using uncompressed: {}", compressionType, e.getMessage());
            return data; // Fallback to uncompressed
        }
    }
    
    /**
     * Decompress a ByteBuffer using the specified compression type.
     * Returns the original buffer if compression is NONE.
     * 
     * @param compressedData The compressed data
     * @param compressionType The compression type that was used
     * @return Decompressed data as ByteBuffer
     * @throws IOException if decompression fails
     */
    public static ByteBuffer decompress(ByteBuffer compressedData, CompressionType compressionType) throws IOException {
        if (compressedData == null || compressionType == CompressionType.NONE) {
            return compressedData;
        }
        
        switch (compressionType) {
            case GZIP:
                return decompressGZIP(compressedData);
            default:
                Logger.warn("Unsupported compression type: {}, treating as uncompressed", compressionType);
                return compressedData;
        }
    }
    
    private static ByteBuffer compressGZIP(ByteBuffer data) throws IOException {
        byte[] input = new byte[data.remaining()];
        data.duplicate().get(input);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(input);
        }
        
        return ByteBuffer.wrap(baos.toByteArray());
    }
    
    private static ByteBuffer decompressGZIP(ByteBuffer compressedData) throws IOException {
        byte[] input = new byte[compressedData.remaining()];
        compressedData.duplicate().get(input);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(input))) {
            byte[] buffer = new byte[4096];
            int len;
            while ((len = gzis.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
        }
        
        return ByteBuffer.wrap(baos.toByteArray());
    }
}