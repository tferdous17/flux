package grpc.services;

import broker.Broker;
import io.grpc.stub.StreamObserver;
import org.tinylog.Logger;
import producer.IntermediaryRecord;
import proto.BrokerToPublisherAck;
import proto.PublishDataToBrokerRequest;
import proto.PublishToBrokerGrpc;
import proto.Status;

import java.io.IOException;
import java.util.List;

public class ProducerService extends PublishToBrokerGrpc.PublishToBrokerImplBase {
    Broker broker;

    public ProducerService(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void send(PublishDataToBrokerRequest req, StreamObserver<BrokerToPublisherAck> responseObserver) {
        BrokerToPublisherAck.Builder ackBuilder = BrokerToPublisherAck.newBuilder();
        List<IntermediaryRecord> records = req
                .getRecordsList()
                .stream()
                .map(record -> new IntermediaryRecord(
                                record.getTargetPartition(),
                                record.getData().toByteArray()
                        )
                )
                .toList();

        try {
            Logger.info("Producing messages");
            int recordOffset = broker.produceMessages(records);
            ackBuilder
                    .setAcknowledgement("ACK: Data received successfully.")
                    .setStatus(Status.SUCCESS)
                    .setRecordOffset(recordOffset);

        } catch (IOException e) {
            // will need logic in the future to differentiate between transient and
            // permanent failures
            // producer will need to explicitly handle these failures and possibly retry
            ackBuilder
                    .setAcknowledgement("ERR: " + e.getMessage())
                    .setStatus(Status.TRANSIENT_FAILURE)
                    .setRecordOffset(-1);
        }

        responseObserver.onNext(ackBuilder.build()); // this just sends the response back to the client
        responseObserver.onCompleted(); // lets the client know there are no more messages after this
    }
}
