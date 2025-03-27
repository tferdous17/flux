package consumer;

import commons.headers.Headers;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.tinylog.Logger;
import proto.*;

import java.time.Duration;
import java.util.Arrays;

public class FluxConsumer<K, V> implements Consumer {
    private final ConsumerServiceGrpc.ConsumerServiceBlockingStub blockingStub;
    private ManagedChannel channel;
    private int currentOffset = 0;

    public FluxConsumer() {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        blockingStub = ConsumerServiceGrpc.newBlockingStub(channel);
    }

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
    public void poll(Duration timeout) {
        FetchMessageRequest req = FetchMessageRequest
                .newBuilder()
                .setStartingOffset(currentOffset)
                .build();

        FetchMessageResponse response;
        try {
            // now handle response and deserialize it
            response = blockingStub.fetchMessage(req);
            Message msg = response.getMessage();
            if (msg == null) {
                System.out.println("No more messages to read.");
            }
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    msg.getTopic(),
                    msg.getPartition(),
                    msg.getOffset(),
                    msg.getTimestamp(),
                    msg.getKey(),
                    msg.getValue(),
                    new Headers()
            );

            System.out.println("\nPRINTING CONSUMER RECORD: \n" + record + "\n");
            currentOffset++;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }
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
