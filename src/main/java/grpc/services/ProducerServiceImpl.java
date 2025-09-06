package grpc.services;

import commons.compression.CompressionType;
import server.internal.Broker;
import io.grpc.stub.StreamObserver;
import org.tinylog.Logger;
import producer.IntermediaryRecord;
import proto.BrokerToPublisherAck;
import proto.PublishDataToBrokerRequest;
import proto.PublishToBrokerGrpc;
import proto.Status;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ProducerServiceImpl extends PublishToBrokerGrpc.PublishToBrokerImplBase {
    Broker broker;

    public ProducerServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void send(PublishDataToBrokerRequest req, StreamObserver<BrokerToPublisherAck> responseObserver) {
        BrokerToPublisherAck.Builder ackBuilder = BrokerToPublisherAck.newBuilder();
        
        // Extract compression type from request
        CompressionType compressionType = CompressionType.fromId(req.getCompressionType().getNumber());
        Logger.debug("Received request with compression type: {}", compressionType);
        
        List<IntermediaryRecord> records;
        
        try {
            // Check if data is compressed
            if (req.getIsCompressed() && !req.getCompressedData().isEmpty()) {
                Logger.info("Decompressing batch data using {}", compressionType);
                
                // Decompress the data
                ByteBuffer compressedBuffer = req.getCompressedData().asReadOnlyByteBuffer();
                ByteBuffer decompressedBuffer = commons.compression.CompressionUtils.decompress(
                    compressedBuffer, compressionType);
                
                // Parse decompressed records
                records = parseRecordsFromBytes(decompressedBuffer.array());
                
                Logger.info("Successfully decompressed and parsed {} records", records.size());
            } else {
                // Handle uncompressed data (legacy path)
                records = req
                        .getRecordsList()
                        .stream()
                        .map(record -> {
                            String topic = record.getTopic();
                            if (topic == null || topic.isEmpty()) {
                                throw new IllegalArgumentException("Topic name is required for all records");
                            }
                            return new IntermediaryRecord(
                                    topic,
                                    record.getTargetPartition(),
                                    record.getData().toByteArray()
                            );
                        })
                        .toList();
            }

            Logger.info("Producing {} messages with compression: {}", records.size(), compressionType);
            int recordOffset = broker.produceMessages(records, compressionType);
            ackBuilder
                    .setAcknowledgement("ACK: Data received successfully.")
                    .setStatus(Status.SUCCESS)
                    .setRecordOffset(recordOffset);

        } catch (IOException e) {
            // will need logic in the future to differentiate between transient and
            // permanent failures
            // producer will need to explicitly handle these failures and possibly retry
            Logger.error("Failed to process request: {}", e.getMessage());
            ackBuilder
                    .setAcknowledgement("ERR: " + e.getMessage())
                    .setStatus(Status.TRANSIENT_FAILURE)
                    .setRecordOffset(-1);
        } catch (Exception e) {
            Logger.error("Unexpected error processing request: {}", e.getMessage());
            ackBuilder
                    .setAcknowledgement("ERR: " + e.getMessage())
                    .setStatus(Status.PERMANENT_FAILURE)
                    .setRecordOffset(-1);
        }

        responseObserver.onNext(ackBuilder.build()); // this just sends the response back to the client
        responseObserver.onCompleted(); // lets the client know there are no more messages after this
    }
    
    private List<IntermediaryRecord> parseRecordsFromBytes(byte[] data) throws IOException {
        List<IntermediaryRecord> records = new ArrayList<>();
        ByteArrayInputStream stream = new ByteArrayInputStream(data);
        
        while (stream.available() > 0) {
            proto.Record record = proto.Record.parseDelimitedFrom(stream);
            if (record == null) break; // End of data
            
            String topic = record.getTopic();
            if (topic == null || topic.isEmpty()) {
                throw new IllegalArgumentException("Topic name is required for all records");
            }
            
            records.add(new IntermediaryRecord(
                topic,
                record.getTargetPartition(),
                record.getData().toByteArray()
            ));
        }
        
        return records;
    }
}
