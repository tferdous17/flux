package consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

public interface Consumer {
//    void assign(Collection<TopicPartition> partitions);
    void subscribe(String partitionID); //Collection<String> topics maybe?

    void unsubscribe(String partitionID);

    PollResult poll(Duration timeout);

    void commit(String partitionID,  long offset);


}
