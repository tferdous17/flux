package producer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import commons.FluxExecutor;
import commons.utils.PartitionSelector;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import metadata.InMemoryTopicMetadataRepository;
import metadata.Metadata;
import metadata.MetadataListener;
import metadata.snapshots.ClusterSnapshot;
import org.tinylog.Logger;
import proto.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class FluxProducer<K, V> implements Producer, MetadataListener {
    private final PublishToBrokerGrpc.PublishToBrokerFutureStub publishToBrokerFutureStub;
    private final MetadataServiceGrpc.MetadataServiceFutureStub metadataFutureStub;
    private String bootstrapServer = "localhost:50051"; // default broker addr to send records to
    private ManagedChannel channel;
    List<IntermediaryRecord> buffer;
    private AtomicReference<ClusterSnapshot> cachedClusterMetadata; // read-heavy

    public FluxProducer(Properties props, long initialFlushDelay, long flushDelayInterval) {
        // Props will be used to select which broker a producer would like to send to
        // ! Will not work until Controller + Partition Distribution tickets are complete as all partitions are in first broker by default
        if (props.containsKey("bootstrap.servers")) {
            // TODO: Technically some input validation would be good here
            bootstrapServer = props.get("bootstrap.servers").toString();
        }

        channel = Grpc.newChannelBuilder(bootstrapServer, InsecureChannelCredentials.create()).build();
        publishToBrokerFutureStub = PublishToBrokerGrpc.newFutureStub(channel);
        metadataFutureStub = MetadataServiceGrpc.newFutureStub(channel);
        buffer = new ArrayList<>();

        FluxExecutor
                .getSchedulerService()
                .scheduleWithFixedDelay(this::flushBuffer, initialFlushDelay, flushDelayInterval, TimeUnit.SECONDS);

        cachedClusterMetadata = Metadata.getInstance().getClusterMetadataSnapshot();
        Metadata.getInstance().addListener(this);
    }

    public FluxProducer(long flushDelayInterval) {
        this(new Properties(), 60, flushDelayInterval);
    }

    public FluxProducer() {
        this(new Properties(), 60, 60);
    }

    public void printMetadataForTesting() {
        System.out.println(cachedClusterMetadata);
    }

    @Override
    public void send(ProducerRecord record) throws IOException {
        Object key = record.getKey() == null ? "" : record.getKey();
        Object value = record.getValue() == null ? "" : record.getValue();

        int currentNumBrokerPartitions = cachedClusterMetadata
                .get()
                .brokers()
                .get(bootstrapServer)
                .numPartitions();


        // Determine target partition, and then serialize.
        int targetPartition = PartitionSelector.getPartitionNumberForRecord(
                InMemoryTopicMetadataRepository.getInstance(),
                record.getPartitionNumber(),
                key.toString(),
                record.getTopic(),
                currentNumBrokerPartitions
        );
        System.out.println("Record has target partition = " + targetPartition);
        // Serialize data and convert to ByteString (gRPC only takes this form for byte data)
        byte[] serializedData = ProducerRecordCodec.serialize(record, key.getClass(), value.getClass());

        String topicName = record.getTopic();
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name is required for all records");
        }
        buffer.add(new IntermediaryRecord(topicName, targetPartition, serializedData));
        Logger.info("Record added to buffer.");
    }

    // NOTE: ONLY USE THIS FOR TESTING
    public void forceFlush() {
        flushBuffer();
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }

        Logger.info("COMMENCING BUFFER FLUSH.");

        // Create deep copy/snapshot of the buffer so we can send it in our request and clear the actual buffer
        // This allows us to continue appending to the buffer w/o messing with the data inside the publish request
        List<IntermediaryRecord> bufferSnapshot = new ArrayList<>(buffer);
        buffer.clear();

        PublishDataToBrokerRequest request = PublishDataToBrokerRequest
                .newBuilder()
                .addAllRecords(
                        bufferSnapshot
                        .stream()
                        .map(r -> proto.Record
                                .newBuilder()
                                .setTargetPartition(r.targetPartition())
                                .setData(ByteString.copyFrom(r.data()))
                                .setTopic(r.topicName())
                                .build()
                        )
                        .toList()
                )
                .build();

        System.out.println("SIZE OF REQ: " + request.getSerializedSize());

        // Send the request and receive the response (acknowledgement)
        Logger.info("SENDING OVER BATCH OF RECORDS.");
        ListenableFuture<BrokerToPublisherAck> response = publishToBrokerFutureStub.send(request);
        Futures.addCallback(response, new FutureCallback<BrokerToPublisherAck>() {
            @Override
            public void onSuccess(BrokerToPublisherAck response) {
                Logger.info("Received BrokerToPublisherAck: Acknowledgement={}, Status={}, RecordOffset={}",
                        response.getAcknowledgement(),
                        response.getStatus(),
                        response.getRecordOffset()
                );
            }

            @Override
            public void onFailure(Throwable t) {
                // TODO: Will need actual retry logic eventually.. gRPC should have some built in retry mechanisms to use
                Logger.error(t);
            }
        }, FluxExecutor.getExecutorService());
    }

    @Override
    public void close() {
        flushBuffer();
        channel.shutdownNow();
    }

    @Override
    public void onUpdate(AtomicReference<ClusterSnapshot> newSnapshot) {
        // Using an AtomicReference allows us to take advantage of hardware-level instructions, such as compare_and_swap,
        // to ensure thread safety on our metadata cache while avoiding the use of locks
        // (because locking can incur overhead and dampen performance a bit in multithreaded environments)
        cachedClusterMetadata.set(newSnapshot.get());
    }
}
