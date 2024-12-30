package commons.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import commons.headers.Headers;
import producer.ProducerRecord;

import java.util.Optional;

public class SerializedProducerRecord {
    private static Kryo kryo;

    static {
        // All classes will share a single Kryo object.
        kryo = new Kryo();

        // Kyro requires each class to be registered for serialization. (Includes nested objects)
        kryo.register(ProducerRecord.class);
//        kryo.register(Optional.class, new SerializedOptional());
//        kryo.register(Headers.class, new SerializedHeaders());
    }

    public static <K,V> byte[] serialize(ProducerRecord<K,V> record) {
        Output output = new Output(4096, -1); // 4 KB is the typical size of memory page
        kryo.writeObject(output, record);
        output.close();
        return output.getBuffer();
    }

    public static <K,V> ProducerRecord<K,V> deserialize(byte[] data, Class<K> keyClass, Class<V> valueClass) {
        Input input = new Input(data);
        ProducerRecord<K, V> record = kryo.readObject(input, ProducerRecord.class);
        input.close();
        return record;
    }
}
