package producer;

import commons.header.Properties;

import java.util.Map;

public class FluxProducer<K, V> implements Producer {
    private Map<String, String> configs;
    // TODO: Include RecordAccumulator once implemented

    public FluxProducer(Map<String, String> configs) {
        this.configs = configs;
    }

    public FluxProducer(Properties props) {
        this.configs = props.getAllProperties();
    }

    @Override
    public boolean send(ProducerRecord record) {
        // TODO: implementation of this method depends on RecordAccumulator to be done
        return false;
    }

    @Override
    public void close() {

    }
}
