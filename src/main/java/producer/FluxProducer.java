package producer;

import com.google.protobuf.ByteString;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FluxProducer<K, V> implements Producer {
    private final PublishToBrokerGrpc.PublishToBrokerBlockingStub blockingStub;
    private final MetadataServiceGrpc.MetadataServiceBlockingStub metadataBlockingStub;
    private ManagedChannel channel;

    List<IntermediaryRecord> buffer;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    public FluxProducer(long initialFlushDelay, long flushDelayInterval) {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        blockingStub = PublishToBrokerGrpc.newBlockingStub(channel);
        metadataBlockingStub = MetadataServiceGrpc.newBlockingStub(channel);
        buffer = new ArrayList<>();
        scheduledExecutorService.scheduleWithFixedDelay(this::flushBuffer, initialFlushDelay, flushDelayInterval, TimeUnit.SECONDS);
    }

    public FluxProducer(long flushDelayInterval) {
        this(60, flushDelayInterval);
    }

    public FluxProducer() {
        this(60, 60);
    }

    @Override
    public void send(ProducerRecord record) throws IOException {
        // need to maintain a buffer of serialized records
        // then once it reaches a particular threshold, flush it by adding all the recs to a grpc req
        // and then sending it over to the broker
        Object key = record.getKey() == null ? "" : record.getKey();
        Object value = record.getValue() == null ? "" : record.getValue();

        // fetch our broker's num of partitions to help determine target partition in round-robin scenario
        FetchBrokerMetadataResponse brokerMetadataResponse = metadataBlockingStub
                .fetchBrokerMetadata(FetchBrokerMetadataRequest.newBuilder().build());

        // Determine target partition, and then serialize.
        int targetPartition = PartitionSelector.getPartitionNumberForRecord(
                InMemoryTopicMetadataRepository.getInstance(),
                record.getPartitionNumber(),
                key.toString(),
                record.getTopic(),
                brokerMetadataResponse.getNumPartitions() // must use this exact ID for testing
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
        Logger.info("COMMENCING BUFFER FLUSH.");

        if (buffer.isEmpty()) {
            return;
        }

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
        BrokerToPublisherAck response;
        try {
            Logger.info("SENDING OVER BATCH OF RECORDS.");
            response = blockingStub.send(request);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        if (response.getStatus().equals(Status.SUCCESS)) {
            buffer.clear();
        }

        System.out.println("\n--------------------------------");
        System.out.println(response.getAcknowledgement());
        System.out.println("Status: " + response.getStatus());
        System.out.println("Record Offset: " + response.getRecordOffset());
        System.out.println("--------------------------------\n");
    }

    @Override
    public void close() {
        flushBuffer();
        channel.shutdownNow();
    }
}
