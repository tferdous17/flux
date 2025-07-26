package metadata;

import broker.Broker;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Note: The real Kafka uses a Metadata API to fetch metadata directly from a given broker using network requests.
 * This will be implemented here soon, but because this is quicker we are currently using an in memory solution like so.
 * TODO: Implement proper Metadata API using gRPC in a separate ticket/PR.
 */
public class InMemoryBrokerMetadataRepository implements BrokerMetadataRepository {
    Set<String> activeBrokerAddresses = new HashSet<>();
    ConcurrentMap<String, Broker> map = new ConcurrentHashMap<>(); // brokerId -> { Broker }

    private InMemoryBrokerMetadataRepository() {
    }

    // Bill Pugh Singleton - refer to InMemoryTopicMetadataRepository for quick explanation
    private static class SingletonHelper {
        private static final InMemoryBrokerMetadataRepository INSTANCE = new InMemoryBrokerMetadataRepository();
    }

    public static InMemoryBrokerMetadataRepository getInstance() {
        return SingletonHelper.INSTANCE;
    }

    @Override
    public void addBroker(String brokerId, Broker broker) {
        map.put(brokerId, broker);
        activeBrokerAddresses.add("%s:%d".formatted(broker.getHost(), broker.getPort()));
    }

    @Override
    public void updateBrokerPartitionCount(String brokerId, int newCount) {

    }


    @Override
    public Set<String> getAllBrokerServerAddresses() {
        return activeBrokerAddresses;
    }

    @Override
    public int getNumberOfPartitionsById(String brokerId) {
        if (!map.containsKey(brokerId)) {
            throw new IllegalArgumentException("Broker ID " + brokerId + " does not exist");
        }
        return map.get(brokerId).getNumPartitions();
    }


}
