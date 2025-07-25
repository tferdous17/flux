package commons.utils;

import commons.IntRange;
import metadata.TopicMetadataRepository;
import producer.MurmurHash2;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class to determine which partition a particular record should go to.
 * Since multiple fields can help determine partition number, the priority ordering is as follows:
 *      1. If a partition # is passed in to the record already, this takes top priority.
 *          1a. Invalid partition numbers automatically get defaulted to round-robin.
 *      2. If no partition number, attempt key-based hashing.
 *      3. If no partition number and no key, default to round-robin.
 *      4. If a topic is also passed in, this just narrows down which partitions we can select for the above operations.
 *
 * Note: The print statements are just for verifying correct selection. Should remove them at a later time when no longer necessary.
 */
public class PartitionSelector {
    private static AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public static int getPartitionNumberForRecord(TopicMetadataRepository topicMetadata, Integer partitionNumber, String key, String topicName, int numPartitions) {
        if (topicName != null && !topicName.isEmpty() && topicMetadata.topicExists(topicName)) {
            return getPartitionNumWhenTopicExists(topicMetadata, partitionNumber, key, topicName);
        } else {
            return getPartitionNumWhenTopicDoesNotExist(topicMetadata, partitionNumber, key, numPartitions);
        }
    }

    private static int getPartitionNumWhenTopicExists(TopicMetadataRepository topicMetadata, Integer partitionNumber, String key, String topicName) {
        IntRange validPartitionIdRange = topicMetadata.getPartitionIdRangeForTopic(topicName);
        int min = validPartitionIdRange.start();
        int max = validPartitionIdRange.end();
        int range = max - min + 1;

        // No partition number, attempt key-based hashing.
        if (partitionNumber == null) {
            // For records with a key, use MurmurHash2 for consistent hashing
            if (key != null && !key.isEmpty()) {
                // Must ensure that it selects a partition within this topic's range.
                return selectPartitionWithinRangeUsingMurmurHash2(key, min, max);
            } else {
                // At this point: Yes topic, no partition #, no key. Default to round-robin within valid range.
                // Jump to bottom of file for example of how this calculation works.
                System.out.println("round robin triggering under topic exists, no partition, no key.");
                System.out.printf("round robin counter = %d minPartition = %d maxPartition = %d range = %d%n", roundRobinCounter.getAndIncrement(), min, max, range);
                return ((roundRobinCounter.getAndIncrement() - min) % range + range) % range + min;
            }
        } else { // Validate partition num is within topic's range of partitions.
            // No need to handle key here because partition num takes priority over key in partition selection.
            if (min <= partitionNumber && partitionNumber <= max) {
                return partitionNumber;
            } else {
                // What if it's out of range? Two subcases here:
                // 1) Fallback to round-robin among this topic's partitions anyway.
                // or 2) Throw InvalidPartitionException.
                System.out.println("round robin triggering under topic exists, passed in partition #, no key.");
                System.out.printf("round robin counter = %d minPartition = %d maxPartition = %d range = %d%n", roundRobinCounter.getAndIncrement(), min, max, range);
                return ((roundRobinCounter.getAndIncrement() - min) % range + range) % range + min;
            }
        }
    }

    private static int getPartitionNumWhenTopicDoesNotExist(TopicMetadataRepository topicMetadata, Integer partitionNumber, String key, int numPartitions) {
        // No partition number either, attempt key-based hashing.
        if (partitionNumber == null) {
            // For records with a key, use MurmurHash2 for consistent hashing
            if (key != null && !key.isEmpty()) {
                return selectPartitionUsingMurmurHash2(key, numPartitions);
            } else {
                // At this point: No topic, no partition #, no key. Default to round-robin among all partitions in system.
                // TODO: Should we insert topic-less records into partitions that belong to a particular topic? How to handle? Come back later
                System.out.println("round robin triggering under NO topic, NO partition, NO key.");
                System.out.printf("round robin counter = %d, numPartitions = %d", roundRobinCounter.getAndIncrement(), numPartitions);
                return roundRobinCounter.getAndIncrement() % numPartitions;
            }
        } else { // Validate partition number
            if (1 <= partitionNumber && partitionNumber <= numPartitions) {
                return partitionNumber;
            } else {
                return roundRobinCounter.getAndIncrement() % numPartitions;
            }
        }
    }

    // If key is null/empty, returns 0 (default partition).
    // Otherwise, returns MurmurHash2(key) % numPartitions.
    public static int selectPartitionUsingMurmurHash2(String key, int numPartitions) {
        if (key == null || key.isEmpty()) {
            return 0;
        }
        int hash = MurmurHash2.hash(key);
        // Turn the hash into a positive number.
        // The & 0x7fffffff operation is a faster way of doing Math.abs()
        // that also handles Integer.MIN_VALUE correctly.
        int positiveHash = hash & 0x7fffffff;
        System.out.printf("Murmur2 hash returning = %d (no range)".formatted(positiveHash % numPartitions));
        return positiveHash % numPartitions;
    }

    public static int selectPartitionWithinRangeUsingMurmurHash2(String key, int min, int max) {
        if (key == null || key.isEmpty()) {
            System.out.println("Empty/null key, returning = " + min);
            return min; // Instead of 0, our default partition will be the partition w/ the min ID
        }
        int hash = MurmurHash2.hash(key);
        int positiveHash = hash & 0x7fffffff;
        System.out.printf("Murmur2 hash returning = %d within range [%d,%d]%n", min + (positiveHash % (max - min + 1)), min, max);
        return min + (positiveHash % (max - min + 1));
    }
}

/**
 * Example: our partition IDs for this topic range from 3..6 and roundRobinCounter returns 7
 * ((7 - 3) % 4 + 4) % 4 + 3
 * = ((4) % 4 + 4) % 4 + 3
 * = (0 + 4) % 4 + 3
 * = 4 % 4 + 3
 * = 0 + 3
 * = 3
 *
 * Example: our partition IDs for this topic range from 3..6 and roundRobinCounter returns 4
 * ((4 - 3) % 4 + 4) % 4 + 3
 * = ((1 % 4) + 4) % 4 + 3
 * = (1 + 4) % 4 + 3
 * = 5 % 4 + 3 = 1 + 3
 * = 4
 */