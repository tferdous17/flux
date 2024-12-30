package producer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import commons.headers.Headers;

import java.util.Optional;

public class SerializedProducerRecord {
    private static Kryo kyro;

    static {
        // All classes will share a single Kyro object.
        kyro = new Kryo();

        // Kyro requires each class to be registered for serialization. (Includes nested objects)
        kyro.register(ProducerRecord.class);
//        kryo.register(Optional.class, new OptionalSerializer());
//        kryo.register(Headers.class, new HeadersSerializer());
    }

    public static <K,V> byte[] serialize(ProducerRecord<K,V> record) {

    }

    public static <K,V> ProducerRecord<K,V> deserialize(byte[] data, Class<K> keyClass, Class<V> valueClass) {

    }
}
