package producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import commons.header.Header;
import commons.headers.Headers;
import commons.serializers.OptionalSerializer;
import commons.serializers.HeaderSerializer;
import commons.serializers.HeadersSerializer;
import commons.serializers.ProducerRecordSerializer;

import java.nio.ByteBuffer;
import java.util.Optional;

public class SerializedProducerRecord {
    private static Kryo kryo;
    static final int INITIAL_BUFFER_SIZE = 4096;

    static {
        // All classes will share a single Kryo object.
        kryo = new Kryo();

        // Kyro requires each class to be registered for serialization. (Includes nested objects)
        kryo.register(Optional.class, new OptionalSerializer());
        kryo.register(Headers.class, new HeadersSerializer());
        kryo.register(Header.class, new HeaderSerializer());
    }

    public static <K,V> byte[] serialize(ProducerRecord<K,V> record, Class<K> keyClass, Class<V> valueClass) {
        // Dynamically register the generic types
        kryo.register(ProducerRecord.class, new ProducerRecordSerializer<>(keyClass, valueClass));

        final Output output = new Output(INITIAL_BUFFER_SIZE, -1); // 4 KB is the typical size of memory page
        kryo.writeObject(output, record);
        output.close();

        byte[] serializedData = output.getBuffer();
        int dataSize = serializedData.length;

        // Both dataSize and offset are 4 bytes => 4 * 2 fields => 8 bytes = metadata size.
        int metadataSize = Integer.BYTES * 2;
        ByteBuffer metadataBuffer = ByteBuffer.allocate(metadataSize + dataSize); // Total size + offset

        metadataBuffer.putInt(dataSize);
        metadataBuffer.putInt(INITIAL_BUFFER_SIZE); // Offset (example: for simplicity, it's set to the buffer size)
        metadataBuffer.put(serializedData);

        return metadataBuffer.array();
    }

    public static <K, V> ProducerRecord<K, V> deserialize(byte[] data, Class<K> keyClass, Class<V> valueClass) {
        // Read metadata
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int dataSize = buffer.getInt();
        int offset = buffer.getInt(); // For future use, e.g., key and value offsets

        byte[] recordData = new byte[dataSize];
        buffer.get(recordData);

        // Dynamically register the types (K, V) of Producer Record
        kryo.register(keyClass);
        kryo.register(valueClass);
        Input input = new Input(recordData);
        ProducerRecord<K, V> record = kryo.readObject(input, ProducerRecord.class);
        input.close();
        return record;
    }
}
