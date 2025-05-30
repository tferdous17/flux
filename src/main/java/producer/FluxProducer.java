package producer;

import com.google.protobuf.ByteString;
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

public class FluxProducer<K, V> implements Producer {
    private final PublishToBrokerGrpc.PublishToBrokerBlockingStub blockingStub;
    private ManagedChannel channel;
    RecordAccumulator recordAccumulator = new RecordAccumulator();
    List<byte[]> buffer;
    private final int BUFFER_RECORD_THRESHOLD = 10_000; // adjust as needed

    public FluxProducer() {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        blockingStub = PublishToBrokerGrpc.newBlockingStub(channel);
        buffer = new ArrayList<>();
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

        if (buffer.size() >= BUFFER_RECORD_THRESHOLD) { // rn just flushing based off # of records
            flushBuffer();
        }
    }

    private void flushBuffer() {
        Logger.info("COMMENCING BUFFER FLUSH.");
        List<ByteString> byteStringList = buffer.stream().map(ByteString::copyFrom).toList();

        PublishDataToBrokerRequest request = PublishDataToBrokerRequest
                .newBuilder()
                .addAllData(byteStringList)
                .build();

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
