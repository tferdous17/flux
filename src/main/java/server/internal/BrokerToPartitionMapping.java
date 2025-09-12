package server.internal;

import java.util.List;

public record BrokerToPartitionMapping(String brokerAddr, String topicName, List<Integer> partitionIds) {
}
