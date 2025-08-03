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
import org.tinylog.Logger;
import proto.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.StampedLock;

public class FluxProducer<K, V> implements Producer {
    private final PublishToBrokerGrpc.PublishToBrokerFutureStub publishToBrokerFutureStub;
    private final MetadataServiceGrpc.MetadataServiceFutureStub metadataFutureStub;
    private ManagedChannel channel;
    List<IntermediaryRecord> buffer;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(2);

    private FetchBrokerMetadataResponse cachedBrokerMetadata; // read-heavy
    private final StampedLock stampedLock = new StampedLock();

    public FluxProducer(long initialFlushDelay, long flushDelayInterval) {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        publishToBrokerFutureStub = PublishToBrokerGrpc.newFutureStub(channel);
        metadataFutureStub = MetadataServiceGrpc.newFutureStub(channel);
        buffer = new ArrayList<>();

        scheduledExecutorService.scheduleWithFixedDelay(this::flushBuffer, initialFlushDelay, flushDelayInterval, TimeUnit.SECONDS);
        scheduledExecutorService.scheduleWithFixedDelay(this::refreshCachedMetadata, 5, 5, TimeUnit.MINUTES);

        // fetch metadata upon initialization and cache the metadata it needs later
        FetchBrokerMetadataRequest request = FetchBrokerMetadataRequest.newBuilder().build();
        try {
            cachedBrokerMetadata = metadataFutureStub.fetchBrokerMetadata(request).get();
            printMetadataForTesting();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public FluxProducer(long flushDelayInterval) {
        this(60, flushDelayInterval);
    }

    public FluxProducer() {
        this(60, 60);
    }

    public void printMetadataForTesting() {
        System.out.println(cachedBrokerMetadata);
    }

    /**
     * To refresh the cache, we will be asynchronously fetching the metadata and upon success we will obtain
     * an exclusive write lock and replace our cache w/ the updated metadata.
     */
    private void refreshCachedMetadata() {
        Logger.info("REFRESHING CACHED BROKER METADATA");
        FetchBrokerMetadataRequest request = FetchBrokerMetadataRequest.newBuilder().build();
        ListenableFuture<FetchBrokerMetadataResponse> future = metadataFutureStub.fetchBrokerMetadata(request);
        Futures.addCallback(future, new FutureCallback<FetchBrokerMetadataResponse>() {
            @Override
            public void onSuccess(FetchBrokerMetadataResponse result) {
                long stamp = stampedLock.writeLock();
                try {
                    cachedBrokerMetadata = result;
                    Logger.info("SUCCESSFULLY REFRESHED CACHED BROKER METADATA");
                } finally {
                    stampedLock.unlockWrite(stamp);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                Logger.error(t);
            }
        }, FluxExecutor.getExecutorService());
    }

    /**
     *  We are doing optimistic reads since our metadata cache is read-heavy, and optimistic reads
     *  allow us to perform reads without obtaining a full lock--ideal for scenarios like this.
     *  NOTE: This assumes we will have minimal contention on our cache (i.e., read-write conflicts are expected to NOT be common)
     *
     *  If the `if` block triggers, it means that a conflict has occurred.
     *  I.e., between the time of us doing the optimistic read and validating, another thread acquired a write lock
     *  and modified the shared resource. In this case, we will fall back by acquiring an exclusive read lock on the cache.
     *  Note there exists other fallback mechanisms like repeatedly retrying the read operation or exponential backoff to avoid overwhelming the system w/ retries
     */
    private int retrieveCurrentNumBrokerPartitions() {
        long stamp = stampedLock.tryOptimisticRead();
        int currentNumBrokerPartitions = cachedBrokerMetadata.getNumPartitions();
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                currentNumBrokerPartitions = cachedBrokerMetadata.getNumPartitions();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return currentNumBrokerPartitions;
    }

    @Override
    public void send(ProducerRecord record) throws IOException {
        Object key = record.getKey() == null ? "" : record.getKey();
        Object value = record.getValue() == null ? "" : record.getValue();

        int currentNumBrokerPartitions = retrieveCurrentNumBrokerPartitions();

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

        buffer.add(new IntermediaryRecord(targetPartition, serializedData));
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
                        response.getRecordOffset());
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
}
