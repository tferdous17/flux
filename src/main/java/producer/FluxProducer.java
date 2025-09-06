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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class FluxProducer<K, V> implements Producer, MetadataListener {
    private final PublishToBrokerGrpc.PublishToBrokerFutureStub publishToBrokerFutureStub;
    private final MetadataServiceGrpc.MetadataServiceFutureStub metadataFutureStub;
    private String bootstrapServer = "localhost:50051"; // default broker addr to send records to
    private ManagedChannel channel;
    private RecordAccumulator accumulator;
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

        cachedClusterMetadata = Metadata.getInstance().getClusterMetadataSnapshot();
        
        // Get number of partitions for accumulator initialization
        int currentNumBrokerPartitions = cachedClusterMetadata
                .get()
                .brokers()
                .get(bootstrapServer)
                .numPartitions();
        
        // Initialize accumulator with ProducerConfig from properties
        ProducerConfig config = new ProducerConfig(props);
        accumulator = new RecordAccumulator(config, currentNumBrokerPartitions);

        // Use linger.ms from config for scheduling interval (convert to milliseconds)
        FluxExecutor
                .getSchedulerService()
                .scheduleWithFixedDelay(this::flushBuffer, config.getLingerMs(), config.getLingerMs(), TimeUnit.MILLISECONDS);

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

        String topicName = record.getTopic();
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name is required for all records");
        }

        // Serialize data (RecordAccumulator will handle partition selection internally)
        byte[] serializedData = ProducerRecordCodec.serialize(record, key.getClass(), value.getClass());

        try {
            // Use accumulator to append record - it handles partitioning internally
            accumulator.append(serializedData);
            Logger.info("Record added to accumulator.");
        } catch (IllegalStateException e) {
            // Handle memory limit exceeded
            Logger.error("Failed to send record - memory limit exceeded: {}", e.getMessage());
            throw new IOException("Cannot send record: " + e.getMessage(), e);
        }
    }

    // NOTE: ONLY USE THIS FOR TESTING
    public void forceFlush() {
        flushBuffer();
    }

    private void flushBuffer() {
        try {
            // Get topic-partitions with ready batches
            List<TopicPartition> readyPartitions = accumulator.ready();
            if (readyPartitions.isEmpty()) {
                return;
            }

            Logger.info("COMMENCING BUFFER FLUSH for {} ready topic-partitions.", readyPartitions.size());

            // Drain ready batches from accumulator
            Map<TopicPartition, RecordBatch> drainedBatches = accumulator.drain(readyPartitions);

            // Convert batches to records for gRPC request
            List<proto.Record> records = new ArrayList<>();
            for (Map.Entry<TopicPartition, RecordBatch> entry : drainedBatches.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                RecordBatch batch = entry.getValue();
                
                // Get batch data (will be compressed if enabled)
                byte[] batchData = batch.getData();
                
                proto.Record record = proto.Record
                        .newBuilder()
                        .setTopic(topicPartition.getTopic())
                        .setTargetPartition(topicPartition.getPartition())
                        .setData(ByteString.copyFrom(batchData))
                        .build();
                records.add(record);
                
                Logger.info("Adding batch from {} with {} bytes (compressed: {})", 
                           topicPartition, batchData.length, batch.isCompressed());
            }

            PublishDataToBrokerRequest request = PublishDataToBrokerRequest
                    .newBuilder()
                    .addAllRecords(records)
                    .build();

            System.out.println("SIZE OF REQ: " + request.getSerializedSize());

            // Send the request and receive the response (acknowledgement)
            Logger.info("SENDING BATCH OF {} RECORDS FROM {} PARTITIONS.", records.size(), drainedBatches.size());
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
                    Logger.error("Failed to send batch to broker", t);
                }
            }, FluxExecutor.getExecutorService());
            
        } catch (IOException e) {
            Logger.error("Error during buffer flush", e);
        }
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
