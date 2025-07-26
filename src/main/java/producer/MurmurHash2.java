package producer;

import java.nio.charset.StandardCharsets;

public final class MurmurHash2 {

    private MurmurHash2() {
        throw new AssertionError("Cannot be instantiated.");
    }

    // 32-bit MurmurHash2 implementation for String keys
    public static int hash(String data) {
        if (data == null)
            return 0;
        // different encoding can be used, but UTF-8 is standard and widely supported
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;

        // Algorithm constants
        int seed = 0x9747b28c;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ length;
        int len_4 = length >> 2;

        // Body: Process 4-byte blocks
        for (int i = 0; i < len_4; i++) {
            int i4 = i << 2;
            int k = (bytes[i4 + 0] & 0xff)
                    | ((bytes[i4 + 1] & 0xff) << 8)
                    | ((bytes[i4 + 2] & 0xff) << 16)
                    | ((bytes[i4 + 3] & 0xff) << 24);

            // Body: Mix the block
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Tail: Mix the last 1-3 bytes from the first unprocessed byte
        int tail = len_4 << 2; // first unprocessed byte
        int left = length - tail; // 0-3 bytes
        if (left != 0) {
            switch (left) {
                case 3: h ^= (bytes[tail + 2] & 0xff) << 16;
                case 2: h ^= (bytes[tail + 1] & 0xff) << 8;
                case 1: h ^= (bytes[tail] & 0xff);
            }
            h *= m;
        }

        // Final avalanche - force all bits to avalanche
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }
}
