package commons.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import commons.headers.Headers;
import producer.ProducerRecord;

public class ProducerRecordSerializer<K,V> extends Serializer<ProducerRecord<K,V>> {
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    public ProducerRecordSerializer(Class<K> keyClass, Class<V> valueClass) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }

    @Override
    public void write(Kryo kryo, Output output, ProducerRecord<K, V> record) {
        output.writeString(record.getTopic());
        output.writeLong(record.getTimestamp());

        // Optional fields
        kryo.writeObjectOrNull(output, record.getPartitionNumber(), Integer.class);
        kryo.writeObjectOrNull(output, record.getKey(), keyClass);

        kryo.writeObject(output, record.getValue());
        kryo.writeObject(output, record.getHeaders());
    }

    @Override
    public ProducerRecord<K, V> read(Kryo kryo, Input input, Class<? extends ProducerRecord<K, V>> aClass) {
        String topic = input.readString();
        Long timestamp = input.readLong();
        Integer partitionNumber = kryo.readObjectOrNull(input, Integer.class);
        K key = kryo.readObjectOrNull(input, keyClass);
        V value = kryo.readObject(input, valueClass);
        Headers headers = kryo.readObject(input, Headers.class);

        return new ProducerRecord<>(topic, partitionNumber, timestamp, key, value, headers);
    }

}
