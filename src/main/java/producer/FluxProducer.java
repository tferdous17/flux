package producer;

import com.google.protobuf.ByteString;
import commons.headers.Headers;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.tinylog.Logger;
import proto.BrokerToPublisherAck;
import proto.PublishDataToBrokerRequest;
import proto.PublishToBrokerGrpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FluxProducer<K, V> implements Producer {
    private final PublishToBrokerGrpc.PublishToBrokerBlockingStub blockingStub;
    private ManagedChannel channel;
    List<byte[]> buffer;
    private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

    public FluxProducer(long initialFlushDelay, long flushDelayInterval) {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        blockingStub = PublishToBrokerGrpc.newBlockingStub(channel);
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

        // Serialize data and convert to ByteString (gRPC only takes this form for byte data)
        byte[] serializedData = ProducerRecordCodec.serialize(record, key.getClass(), value.getClass());
        buffer.add(serializedData);
        Logger.info("Record added to buffer.");
    }

    private void flushBuffer() {
        Logger.info("COMMENCING BUFFER FLUSH.");

        if (buffer.isEmpty()) {
            return;
        }

        List<byte[]> bufferSnapshot = new ArrayList<>();
        for (byte[] b : buffer) {
            bufferSnapshot.add(b);
        }
        buffer.clear();

        List<ByteString> byteStringList = bufferSnapshot.stream().map(ByteString::copyFrom).toList();

        PublishDataToBrokerRequest request = PublishDataToBrokerRequest
                .newBuilder()
                .addAllData(byteStringList)
                .build();

        System.out.println("SIZE OF REQ: " + request.getSerializedSize());

        // Send the request and receive the response (acknowledgement)
        BrokerToPublisherAck response;
        try {
            Logger.info("SENDING OVER BATCH OF RECORDS.");
            response = blockingStub.send(request);
            buffer.clear();
        } catch (Exception e) {
            e.printStackTrace();
            return;
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
