package producer;

import commons.header.Properties;

import java.io.IOException;
import java.util.Map;

public class FluxProducer<K, V> implements Producer {
    private Map<String, String> configs;
    RecordAccumulator recordAccumulator = new RecordAccumulator();

    public FluxProducer(Map<String, String> configs) {
        this.configs = configs;
    }

    public FluxProducer(Properties props) {
        this.configs = props.getAllProperties();
    }

    @Override
    public void send(ProducerRecord record) throws IOException {
        byte[] serializedData = ProducerRecordCodec.serialize(record, record.getKey().getClass(), record.getValue().getClass());
        recordAccumulator.append(serializedData);
    }

    @Override
    public void close() {
        //TODO: RecordAccumulator should flush any remaining records
    }
}
