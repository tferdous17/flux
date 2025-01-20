package consumer;

import java.time.Duration;
import java.util.Collection;

public class FluxConsumer<K, V> implements ProducerInterface {

    @Override
    public void poll(Duration timeout) {

    }

    @Override
    public void subscribe(Collection<String> topics) {

    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitAsync() {

    }
}
