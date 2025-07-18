package producer;

import java.nio.charset.StandardCharsets;

public final class MurmurHash2 {

    private MurmurHash2() {
        // This class should not be instantiated.
    }

    // 32-bit MurmurHash2 implementation for String keys
    public static int hash(String data) {
        if (data == null) return 0;
        // different encoding can be used, but UTF-8 is standard and widely supported
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        int seed = 0x9747b28c;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ length;
        int len_4 = length >> 2;

        for (int i = 0; i < len_4; i++) {
            int i4 = i << 2;
            int k = (bytes[i4 + 0] & 0xff)
                  | ((bytes[i4 + 1] & 0xff) << 8)
                  | ((bytes[i4 + 2] & 0xff) << 16)
                  | ((bytes[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        int len_m = len_4 << 2;
        int left = length - len_m;
        if (left != 0) {
            if (left >= 3) h ^= (bytes[length - 3] & 0xff) << 16;
            if (left >= 2) h ^= (bytes[length - 2] & 0xff) << 8;
            if (left >= 1) h ^= (bytes[length - 1] & 0xff);
            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    /**
     * Standardized partition selection utility.
     * If key is null/empty, returns 0 (default partition).
     * Otherwise, returns MurmurHash2(key) % numPartitions.
     */
    public static int selectPartition(String key, int numPartitions) {
        if (key == null || key.isEmpty()) {
            return 0;
        }
        int hash = MurmurHash2.hash(key);
        // Turn the hash into a positive number.
        // The & 0x7fffffff operation is a faster way of doing Math.abs()
        // that also handles Integer.MIN_VALUE correctly.
        int positiveHash = hash & 0x7fffffff;
        return positiveHash % numPartitions;
    }
}
