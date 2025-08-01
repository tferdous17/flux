package metadata;

import broker.Broker;

import java.util.Set;

public interface BrokerMetadataRepository {
    void addBroker(String brokerId, Broker broker);
    void updateBrokerPartitionCount(String brokerId, int newCount);
    Set<String> getAllBrokerServerAddresses();
    int getNumberOfPartitionsById(String brokerId);
}
