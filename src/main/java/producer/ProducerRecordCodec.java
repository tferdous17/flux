package producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import commons.header.Header;
import commons.headers.Headers;
import commons.serializers.*;
import org.tinylog.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

public class ProducerRecordCodec {
    private static Kryo kryo;
    static final int INITIAL_BUFFER_SIZE = 4096;
    static int headerSizeInBytes = Integer.BYTES * 3;

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

        // only want the occupied space instead of the entire buffer which can contain empty space
        byte[] serializedData = Arrays.copyOf(output.getBuffer(), output.position());
        int recordSize = serializedData.length;

        // Both byteOffset and recordSize are 4 bytes => 4 * 2 fields => 8 bytes = header size.
        ByteBuffer completeRecordBuffer = ByteBuffer.allocate(headerSizeInBytes + recordSize);

        completeRecordBuffer.putInt(0); // first 4 bytes for record offset
        completeRecordBuffer.putInt(0); // next 4 bytes for the byte offset (will fill in on Broker side)
        completeRecordBuffer.putInt(recordSize); // next 4 bytes are record size
        completeRecordBuffer.put(serializedData); // rest is data

        Logger.info(Arrays.toString(completeRecordBuffer.array()));

        return completeRecordBuffer.array();
    }

    public static <K, V> ProducerRecord<K, V> deserialize(byte[] data, Class<K> keyClass, Class<V> valueClass) {
        // Read metadata
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int recordOffset = buffer.getInt();
        int byteOffset = buffer.getInt();
        int recordSize = buffer.getInt();

        System.out.println("PRORECCODEC LOG: RECORD SIZE = " + recordSize);
        byte[] recordData = new byte[recordSize];
        buffer.get(headerSizeInBytes, recordData);

        // Dynamically register the types (K, V) of Producer Record
        kryo.register(keyClass);
        kryo.register(valueClass);
        Input input = new Input(recordData);
        ProducerRecord<K, V> record = kryo.readObject(input, ProducerRecord.class);
        input.close();
        return record;
    }
}
