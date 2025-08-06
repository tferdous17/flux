package consumer;

import java.time.Duration;
import java.util.Collection;

public interface Consumer {
    void subscribe(Collection<String> topics);
    void unsubscribe();
    PollResult poll(Duration timeout);
    void commitOffsets();


}
