package consumer;

import org.tinylog.Logger;

import java.time.Duration;

public class FluxConsumer<K, V> implements ConsumerInterface {
/*
    All of these methods below, need a gRPC implementation...
    I will provide pseudocode given we don't have gRPC setup yet.
 */
    @Override
    public void subscribe(String partitionID) {
        //  Create a gRPC request to sub to a partitionID/ list of topics
        /*
            SubscribeRequest request = SubscribeRequest.newBuilder()
            .setPartitionId(partitionId)
            .setConsumerId(this.consumerId) // Unique ID for this consumer
            .build();
         */

        // Send a request to broker afterward:
        // SubscribeResponse response = brokerStub.subscribe(request);

        // Logger messages
        Logger.info("Subbed to partition" + partitionID);
        Logger.warn("Failed to sub to partition" + partitionID);
    }

    @Override
    public void unsubscribe(String partitionID) {
        // Create a gRPC request to unsubscribe from the partition
        /*
        UnsubscribeRequest request = UnsubscribeRequest.newBuilder()
                .setPartitionId(partitionId)
                .setConsumerId(this.consumerId) // Unique ID for this consumer
                .build();
        */

        // Send the request to the broker via gRPC
//        UnsubscribeResponse response = brokerStub.unsubscribe(request);

        // Logger messages
        Logger.info("Unsubscribed to partition" + partitionID);
        Logger.warn("Failed to unsubscribed to partition" + partitionID);
    }

    @Override
    public void fetchMessage(Duration timeout) {
        // Create a gRPC  request to fetch messages
        /*
        FetchRequest request = FetchRequest.newBuilder()
            .setConsumerId(this.consumerId)
            .setTimeoutMs(timeout.toMillis())
            .build();
         */

        // Sends a request to broker
        //  FetchResponse response = brokerStub.fetch(request);

        // Convert gRPC response to list of messages
        /*
        List<Message<K, V>> messages = new ArrayList<>();
        for (MessageProto messageProto : response.getMessagesList()) {
            messages.add(convertFromProto(messageProto)); // Helper method to convert proto to your Message class
        }

        return messages;
         */

        Logger.info("We got messages!");
    }

    @Override
    public void commit(String partitionID,  long offset) {
        // Create a gRPC request to commit to offset
        /*
        CommitRequest request = CommitRequest.newBuilder()
            .setPartitionId(partitionId)
            .setConsumerId(this.consumerId)
            .setOffset(offset)
            .build();
         */

        // Send a request to broker
//        CommitResponse response = brokerStub.commit(request);

        // Logger messages
        Logger.info("Committed offset " + offset + " for partition: " + partitionID);
        Logger.warn("Failed to commit offset for partition: " + partitionID);
    }
}
