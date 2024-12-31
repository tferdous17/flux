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

import java.util.Optional;

public class SerializedProducerRecord {
    private static Kryo kryo;

    static {
        // All classes will share a single Kryo object.
        kryo = new Kryo();

        // Kyro requires each class to be registered for serialization. (Includes nested objects)
        kryo.register(ProducerRecord.class, new ProducerRecordSerializer<>(String.class, Integer.class));
        kryo.register(Optional.class, new OptionalSerializer());
        kryo.register(Headers.class, new HeadersSerializer());
        kryo.register(Header.class, new HeaderSerializer());
    }

    public static <K,V> byte[] serialize(ProducerRecord<K,V> record) {
        final Output output = new Output(4096, -1); // 4 KB is the typical size of memory page
        kryo.writeObject(output, record);
        output.close();
        return output.getBuffer();
    }

    public static <K,V> ProducerRecord<K,V> deserialize(byte[] data, Class<K> keyClass, Class<V> valueClass) {
        // Dynamically register the types (K,V) of Producer Record
        kryo.register(keyClass);
        kryo.register(valueClass);
        Input input = new Input(data);
        ProducerRecord<K, V> record = kryo.readObject(input, ProducerRecord.class);
        input.close();
        return record;
    }
}
