package commons.utils;

import commons.IntRange;
import metadata.TopicMetadataRepository;
import producer.MurmurHash2;

import java.util.concurrent.atomic.AtomicInteger;

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
                return MurmurHash2.selectPartitionWithinRange(key, min, max);
            } else {
                // At this point: Yes topic, no partition #, no key. Default to round-robin within valid range.
                // Jump to bottom of file for example of how this calculation works.
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
                return ((roundRobinCounter.getAndIncrement() - min) % range + range) % range + min;
            }
        }
    }

    private static int getPartitionNumWhenTopicDoesNotExist(TopicMetadataRepository topicMetadata, Integer partitionNumber, String key, int numPartitions) {
        // No partition number either, attempt key-based hashing.
        if (partitionNumber == null) {
            // For records with a key, use MurmurHash2 for consistent hashing
            if (key != null && !key.isEmpty()) {
                return MurmurHash2.selectPartition(key, numPartitions);
            } else {
                // At this point: No topic, no partition #, no key. Default to round-robin among all partitions in system.
                // TODO: Should we insert topic-less records into partitions that belong to a particular topic? How to handle? Come back later
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