package consumer;

import commons.FluxExecutor;
import commons.headers.Headers;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.tinylog.Logger;
import proto.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

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
    public PollResult poll(Duration timeout) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        FetchMessageRequest req = FetchMessageRequest
                .newBuilder()
                .setStartingOffset(currentOffset)
                .build();

        try {
            // push fetch message execution into worker thread
            Future<FetchMessageResponse> future = FluxExecutor.getExecutorService().submit(() -> blockingStub.fetchMessage(req));
            // waits for task to complete for at most the given timeout
            FetchMessageResponse response = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            if (response.getStatus().equals(FetchMessageResponse.Status.READ_COMPLETION)) {
                Logger.info("READ COMPLETION");
                return new PollResult(records, false);
            }

            Message msg = response.getMessage();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    msg.getTopic(),
                    msg.getPartition(),
                    msg.getOffset(),
                    msg.getTimestamp(),
                    msg.getKey(),
                    msg.getValue(),
                    new Headers()
            );
            records.add(record);
            currentOffset = msg.getOffset() + 1;

        } catch (TimeoutException e) {
            // if no data fetched before timeout limit
            Logger.info("Timeout waiting for message");
            return new PollResult(records, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            System.err.println("Fetch failed: " + e.getCause().getMessage());
            return new PollResult(records, false);
        }

        return new PollResult(records, true);
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
