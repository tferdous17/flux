package consumer;

import java.time.Duration;
import java.util.Collection;

public interface ProducerInterface {
//    void assign(Collection<TopicPartition> partitions);
    void poll(Duration timeout);

    void subscribe(Collection<String> topics);

    void unsubscribe();

    void commitSync();

    void commitAsync();


}
