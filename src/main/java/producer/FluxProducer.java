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
    private RecordAccumulator recordAccumulator;
    private AtomicReference<ClusterSnapshot> cachedClusterMetadata; // read-heavy
    private final int maxRetries;
    private final long retryBackoffMs;

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

        FluxExecutor
                .getSchedulerService()
                .scheduleWithFixedDelay(this::flushBuffer, initialFlushDelay, flushDelayInterval, TimeUnit.SECONDS);

        cachedClusterMetadata = Metadata.getInstance().getClusterMetadataSnapshot();
        Metadata.getInstance().addListener(this);
        
        // Initialize configuration from properties
        int numPartitions = cachedClusterMetadata.get().brokers().get(bootstrapServer).numPartitions();
        int batchSize = Integer.parseInt(props.getProperty("batch.size", "10240")); // 10KB default
        long lingerMs = Long.parseLong(props.getProperty("linger.ms", "100")); // 100ms default
        long batchTimeoutMs = Long.parseLong(props.getProperty("batch.timeout.ms", "30000")); // 30s default
        double batchSizeThreshold = Double.parseDouble(props.getProperty("batch.size.threshold", "0.9")); // 90% default
        long bufferMemory = Long.parseLong(props.getProperty("buffer.memory", "33554432")); // 32MB default
        maxRetries = Integer.parseInt(props.getProperty("retries", "3")); // 3 retries default
        retryBackoffMs = Long.parseLong(props.getProperty("retry.backoff.ms", "1000")); // 1s backoff default
        
        recordAccumulator = new RecordAccumulator(batchSize, numPartitions, lingerMs, batchTimeoutMs, batchSizeThreshold, bufferMemory);
        
        Logger.info("FluxProducer initialized - Partitions: {}, BatchSize: {}KB, LingerMs: {}ms, BatchTimeout: {}ms, BatchThreshold: {}, BufferMemory: {}MB, MaxRetries: {}, RetryBackoff: {}ms", 
                   numPartitions, batchSize / 1024, lingerMs, batchTimeoutMs, batchSizeThreshold, bufferMemory / (1024 * 1024), maxRetries, retryBackoffMs);
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
        // Use RecordAccumulator to append the serialized record
        try {
            recordAccumulator.append(serializedData);
            Logger.info("Record appended to RecordAccumulator for partition " + targetPartition);
            
            // Check if we have any ready batches and send them
            checkAndSendReadyBatches();
        } catch (IOException e) {
            Logger.error("Failed to append record to RecordAccumulator: " + e.getMessage(), e);
            throw e;
        }
    }

    // NOTE: ONLY USE THIS FOR TESTING
    public void forceFlush() {
        flushBuffer();
    }

    // NOTE: ONLY USE THIS FOR TESTING
    public RecordAccumulator getAccumulator() {
        return recordAccumulator;
    }

    /**
     * Check for ready batches and send them if any exist
     */
    private void checkAndSendReadyBatches() {
        Map<Integer, RecordBatch> readyBatches = recordAccumulator.getReadyBatches();
        if (!readyBatches.isEmpty()) {
            sendBatches(readyBatches, "READY BATCH SEND");
        }
    }

    /**
     * Flush all batches (scheduled or forced flush)
     */
    private void flushBuffer() {
        // Get all batches from RecordAccumulator
        Map<Integer, RecordBatch> batchesToSend = recordAccumulator.flush();
        
        if (batchesToSend.isEmpty()) {
            return;
        }

        sendBatches(batchesToSend, "SCHEDULED BUFFER FLUSH");
    }

    /**
     * Send batches to broker via gRPC
     */
    private void sendBatches(Map<Integer, RecordBatch> batchesToSend, String operation) {
        Logger.info("COMMENCING " + operation + " - {} batches", batchesToSend.size());

        // Convert batches to IntermediaryRecord format for gRPC
        List<IntermediaryRecord> recordsToSend = recordAccumulator.convertBatchesToIntermediaryRecords(batchesToSend);

        if (recordsToSend.isEmpty()) {
            Logger.info("No records to send in batches");
            return;
        }
        
        // Collect batch IDs for callback notifications
        List<String> batchIds = batchesToSend.values().stream()
                .filter(batch -> batch != null)
                .map(batch -> batch.getBatchId())
                .toList();
        
        // Notify accumulator that batches are being sent
        for (String batchId : batchIds) {
            recordAccumulator.onBatchSending(batchId);
        }

        PublishDataToBrokerRequest request = PublishDataToBrokerRequest
                .newBuilder()
                .addAllRecords(
                        recordsToSend
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

        Logger.info("Sending {} records in {} batches (size: {} bytes)", 
                   recordsToSend.size(), batchesToSend.size(), request.getSerializedSize());

        // Send the request with retry logic, passing batch IDs for callback notifications
        sendBatchesWithRetry(request, recordsToSend.size(), batchesToSend.size(), batchIds, 0);
    }

    /**
     * Send batches with retry logic and exponential backoff
     */
    private void sendBatchesWithRetry(PublishDataToBrokerRequest request, int recordCount, int batchCount, List<String> batchIds, int attemptCount) {
        ListenableFuture<BrokerToPublisherAck> response = publishToBrokerFutureStub.send(request);
        Futures.addCallback(response, new FutureCallback<BrokerToPublisherAck>() {
            @Override
            public void onSuccess(BrokerToPublisherAck response) {
                Logger.info("Successfully sent {} records in {} batches - Acknowledgement={}, Status={}, RecordOffset={}", 
                           recordCount, batchCount,
                           response.getAcknowledgement(),
                           response.getStatus(),
                           response.getRecordOffset());
                
                // Notify accumulator of successful batch sends
                for (String batchId : batchIds) {
                    recordAccumulator.onBatchSendSuccess(batchId, response);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                if (attemptCount < maxRetries) {
                    // Calculate exponential backoff
                    long backoffDelay = retryBackoffMs * (1L << attemptCount);
                    
                    Logger.warn("Batch send failed (attempt {}/{}), retrying in {}ms: {}", 
                               attemptCount + 1, maxRetries + 1, backoffDelay, t.getMessage());
                    
                    // Schedule retry after backoff
                    FluxExecutor.getSchedulerService().schedule(() -> {
                        sendBatchesWithRetry(request, recordCount, batchCount, batchIds, attemptCount + 1);
                    }, backoffDelay, TimeUnit.MILLISECONDS);
                } else {
                    Logger.error("Failed to send {} records in {} batches after {} attempts: {}", 
                                recordCount, batchCount, maxRetries + 1, t.getMessage(), t);
                    
                    // Notify accumulator of failed batch sends after all retries exhausted
                    for (String batchId : batchIds) {
                        recordAccumulator.onBatchSendFailure(batchId, t);
                    }
                    
                    // TODO: Could implement dead letter queue or alerting here
                    // For now, we log the error and continue
                }
            }
        }, FluxExecutor.getExecutorService());
    }

    @Override
    public void close() {
        flushBuffer();
        recordAccumulator.close(); // Close accumulator and release buffer pool
        channel.shutdownNow();
    }

    @Override
    public void onUpdate(AtomicReference<ClusterSnapshot> newSnapshot) {
        // Using an AtomicReference allows us to take advantage of hardware-level instructions, such as compare_and_swap,
        // to ensure thread safety on our metadata cache while avoiding the use of locks
        // (because locking can incur overhead and dampen performance a bit in multithreaded environments)
        ClusterSnapshot oldSnapshot = cachedClusterMetadata.get();
        cachedClusterMetadata.set(newSnapshot.get());
        
        // Check if partition count changed - if so, we may need to flush current batches
        int oldPartitions = oldSnapshot.brokers().get(bootstrapServer).numPartitions();
        int newPartitions = newSnapshot.get().brokers().get(bootstrapServer).numPartitions();
        
        if (oldPartitions != newPartitions) {
            Logger.info("Partition count changed from {} to {} - flushing current batches", 
                       oldPartitions, newPartitions);
            // Flush existing batches before partition count changes
            flushBuffer();
            // Note: RecordAccumulator will automatically handle new partition numbers
            // when records are appended to partitions that don't exist in its map yet
        }
    }
}
