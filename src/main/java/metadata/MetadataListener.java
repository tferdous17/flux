package metadata;

import metadata.snapshots.ClusterSnapshot;

import java.util.concurrent.atomic.AtomicReference;

public interface MetadataListener {
    void onUpdate(AtomicReference<ClusterSnapshot> snapshot);
}
