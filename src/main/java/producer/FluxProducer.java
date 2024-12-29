package producer;

import java.util.Map;
import java.util.Properties;

public class FluxProducer<K, V> implements Producer {
    private Map<String, Object> configs;
    // TODO: Include RecordAccumulator once implemented

    public FluxProducer(Map<String, Object> configs) {
        this.configs = configs;
    }

    // TODO: Replace with Properties object once implemented
//    public FluxProducer(properties obj) {
//
//    }

    @Override
    public boolean send(ProducerRecord record) {
        // TODO: implementation of this method depends on RecordAccumulator to be done
        return false;
    }

    @Override
    public void close() {

    }
}
