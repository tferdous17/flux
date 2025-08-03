package metadata;

import java.util.concurrent.atomic.AtomicReference;

public interface MetadataListener {
    void onUpdate(AtomicReference<BrokerMetadataSnapshot> snapshot);
}
