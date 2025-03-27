package consumer;

import java.time.Duration;
import java.util.Collection;

public interface Consumer {
//    void assign(Collection<TopicPartition> partitions);
    void subscribe(String partitionID); //Collection<String> topics maybe?

    void unsubscribe(String partitionID);

    void poll(Duration timeout);

    void commit(String partitionID,  long offset);


}
