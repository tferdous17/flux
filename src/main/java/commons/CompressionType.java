package commons;

import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public enum CompressionType {
    NONE(0, "none") {
        @Override
        public byte[] compress(byte[] data) {
            return data;
        }

        @Override
        public byte[] decompress(byte[] data) {
            return data;
        }
    },
    
    GZIP(1, "gzip") {
        @Override
        public byte[] compress(byte[] data) throws IOException {
            if (data == null || data.length == 0) {
                return data;
            }
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
                gzipOut.write(data);
            }
            return baos.toByteArray();
        }

        @Override
        public byte[] decompress(byte[] data) throws IOException {
            if (data == null || data.length == 0) {
                return data;
            }
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                 GZIPInputStream gzipIn = new GZIPInputStream(bais)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gzipIn.read(buffer)) != -1) {
                    baos.write(buffer, 0, len);
                }
            }
            return baos.toByteArray();
        }
    },
    
    SNAPPY(2, "snappy") {
        @Override
        public byte[] compress(byte[] data) throws IOException {
            if (data == null || data.length == 0) {
                return data;
            }
            return Snappy.compress(data);
        }

        @Override
        public byte[] decompress(byte[] data) throws IOException {
            if (data == null || data.length == 0) {
                return data;
            }
            return Snappy.uncompress(data);
        }
    },
    
    LZ4(3, "lz4") {
        private final LZ4Factory factory = LZ4Factory.fastestInstance();
        
        @Override
        public byte[] compress(byte[] data) {
            if (data == null || data.length == 0) {
                return data;
            }
            
            LZ4Compressor compressor = factory.fastCompressor();
            int maxCompressedLength = compressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxCompressedLength + 4]; // +4 for original length
            
            // Store original length in first 4 bytes
            compressed[0] = (byte) (data.length >>> 24);
            compressed[1] = (byte) (data.length >>> 16);
            compressed[2] = (byte) (data.length >>> 8);
            compressed[3] = (byte) data.length;
            
            int compressedLength = compressor.compress(data, 0, data.length, 
                                                       compressed, 4, maxCompressedLength);
            
            // Return array with exact size
            byte[] result = new byte[compressedLength + 4];
            System.arraycopy(compressed, 0, result, 0, compressedLength + 4);
            return result;
        }

        @Override
        public byte[] decompress(byte[] data) {
            if (data == null || data.length == 0) {
                return data;
            }
            
            // Read original length from first 4 bytes
            int originalLength = ((data[0] & 0xFF) << 24) |
                                ((data[1] & 0xFF) << 16) |
                                ((data[2] & 0xFF) << 8) |
                                (data[3] & 0xFF);
            
            LZ4SafeDecompressor decompressor = factory.safeDecompressor();
            byte[] decompressed = new byte[originalLength];
            decompressor.decompress(data, 4, data.length - 4, decompressed, 0);
            return decompressed;
        }
    },
    
    ZSTD(4, "zstd") {
        @Override
        public byte[] compress(byte[] data) {
            if (data == null || data.length == 0) {
                return data;
            }
            return Zstd.compress(data);
        }

        @Override
        public byte[] decompress(byte[] data) {
            if (data == null || data.length == 0) {
                return data;
            }
            
            long originalSize = Zstd.decompressedSize(data);
            if (originalSize <= 0 || originalSize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Invalid decompressed size: " + originalSize);
            }
            
            byte[] decompressed = new byte[(int) originalSize];
            Zstd.decompress(decompressed, data);
            return decompressed;
        }
    };
    
    private final int id;
    private final String name;
    
    CompressionType(int id, String name) {
        this.id = id;
        this.name = name;
    }
    
    public int getId() {
        return id;
    }
    
    public String getName() {
        return name;
    }
    
    public abstract byte[] compress(byte[] data) throws IOException;
    
    public abstract byte[] decompress(byte[] data) throws IOException;
    
    public static CompressionType fromId(int id) {
        for (CompressionType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown compression type id: " + id);
    }
    
    public static CompressionType fromName(String name) {
        for (CompressionType type : values()) {
            if (type.name.equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown compression type name: " + name);
    }
}