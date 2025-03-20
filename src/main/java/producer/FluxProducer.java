package producer;

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import proto.BrokerToPublisherAck;
import proto.PublishDataToBrokerRequest;
import proto.PublishToBrokerGrpc;

import java.io.IOException;

public class FluxProducer<K, V> implements Producer {
    private final PublishToBrokerGrpc.PublishToBrokerBlockingStub blockingStub;
    private ManagedChannel channel;
    RecordAccumulator recordAccumulator = new RecordAccumulator();

    public FluxProducer() {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        blockingStub = PublishToBrokerGrpc.newBlockingStub(channel);
    }

    // Batching will be better for when Flux is distributed
    @Override
    public void send(ProducerRecord record) throws IOException {
        byte[] serializedData = ProducerRecordCodec.serialize(record, record.getKey().getClass(), record.getValue().getClass());
        recordAccumulator.append(serializedData);
    }

    // Send record directly to Broker w/o batching.
    @Override
    public void sendDirect(ProducerRecord record) throws IOException {
        Object key = record.getKey() == null ? "" : record.getKey();
        Object value = record.getValue() == null ? "" : record.getValue();

        // Serialize data and convert to ByteString (gRPC only takes this form for byte data)
        byte[] serializedData = ProducerRecordCodec.serialize(record, key.getClass(), value.getClass());
        ByteString data = ByteString.copyFrom(serializedData);

        // Build the request containing our serialized record
        PublishDataToBrokerRequest request = PublishDataToBrokerRequest
                .newBuilder()
                .setData(data)
                .build();

        // Send the request and receive the response (acknowledgement)
        BrokerToPublisherAck response;
        try {
            response = blockingStub.send(request);
        } catch (Exception e) {
            System.out.println(e.getMessage());
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
        //TODO: RecordAccumulator should flush any remaining records
        channel.shutdownNow();
    }
}
