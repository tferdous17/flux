package metadata;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import commons.FluxExecutor;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import metadata.snapshots.BrokerMetadata;
import metadata.snapshots.ClusterSnapshot;
import metadata.snapshots.ControllerMetadata;
import metadata.snapshots.TopicMetadata;
import org.tinylog.Logger;
import proto.*;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Singleton that encapsulates the logic around metadata
 *
 * Utilizes the Observer pattern to notify clients (producers/consumers) of any changes in metadata.
 */
public class Metadata {
    private int refreshIntervalSec;
    private int updateCounter;
    private ManagedChannel channel;
    private final MetadataServiceGrpc.MetadataServiceFutureStub metadataFutureStub;
    private List<MetadataListener> listeners = new ArrayList<>();
    public static AtomicInteger brokerIdCounter = new AtomicInteger(1);
    public static AtomicInteger clusterIdCounter = new AtomicInteger(1);
    private AtomicReference<ClusterSnapshot> currClusterMetadataSnapshot; // Cached cluster metadata

    private Metadata(int refreshIntervalSec) {
        this.refreshIntervalSec = refreshIntervalSec;
        updateCounter = 0;
        // Note: Metadata client should only connect to the Controller node
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        metadataFutureStub = MetadataServiceGrpc.newFutureStub(channel);

        currClusterMetadataSnapshot = new AtomicReference<>(initialMetadataFetch()); // ! This is blocking

        System.out.println("PRINTING CURR CLUSTER METADATA-------------------------");
        System.out.println(currClusterMetadataSnapshot);
        System.out.println("--------------------------------------------------");
        FluxExecutor
                .getSchedulerService()
                .scheduleWithFixedDelay(this::updateMetadata, refreshIntervalSec, refreshIntervalSec, TimeUnit.SECONDS);
    }

    private Metadata() {
        this(100); // default = 5 minutes
    }

    private static class SingletonHelper {
        private static final Metadata INSTANCE = new Metadata();
    }

    public static Metadata getInstance() {
        return SingletonHelper.INSTANCE;
    }

    // Synchronize (mutex) the listeners so there aren't any inconsistencies
    // i.e., don't want a listener to get added at the same time of us notifying our listeners on an update otherwise
    // the new listener won't receive the latest metadata
    public void addListener(MetadataListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
            Logger.info("LISTENER ADDED");
        }
    }

    public void removeListener(MetadataListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
            Logger.info("LISTENER REMOVED");
        }
    }

    public void notifyListeners() {
        Logger.info("METADATA CHANGE DETECTED -- NOTIFYING + UPDATING ALL LISTENERS");
        synchronized (listeners) {
            for (MetadataListener listener : listeners) {
                listener.onUpdate(currClusterMetadataSnapshot);
            }
        }
    }

    // Get the current metadata
    public AtomicReference<ClusterSnapshot> getClusterMetadataSnapshot() {
        return currClusterMetadataSnapshot;
    }

    private ClusterSnapshot initialMetadataFetch() {
        FetchClusterMetadataRequest request = FetchClusterMetadataRequest.newBuilder().build();
        try {
            // Initial fetch will be blocking as to ensure metadata is fully loaded before its used by Producers or Consumers
            FetchClusterMetadataResponse response = metadataFutureStub
                    .fetchClusterMetadata(request)
                    .get();

            return new ClusterSnapshot(
                    ControllerMetadata.from(response.getControllerDetails()),
                    BrokerMetadata.fromMap(response.getBrokerDetailsMap()),
                    TopicMetadata.fromMap(response.getTopicDetailsMap())
            );

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Metadata obj will specifically do PULL based metadata fetching from the controller node(s)
    private void updateMetadata() {
        Logger.info("REFRESHING CLUSTER METADATA SNAPSHOT");

        FetchClusterMetadataRequest request = FetchClusterMetadataRequest.newBuilder().build();
        ListenableFuture<FetchClusterMetadataResponse> future = metadataFutureStub.fetchClusterMetadata(request);

        Futures.addCallback(future, new FutureCallback<FetchClusterMetadataResponse>() {

            @Override
            public void onSuccess(FetchClusterMetadataResponse response) {
                ClusterSnapshot newSnapshot = new ClusterSnapshot(
                        ControllerMetadata.from(response.getControllerDetails()),
                        BrokerMetadata.fromMap(response.getBrokerDetailsMap()),
                        TopicMetadata.fromMap(response.getTopicDetailsMap())
                );

                if (!newSnapshot.equals(currClusterMetadataSnapshot.get())) {
                    currClusterMetadataSnapshot.set(newSnapshot);
                    updateCounter++;
                    notifyListeners();
                    Logger.info("SUCCESSFULLY REFRESHED CACHED CLUSTER METADATA, UPDATE COUNT = %d".formatted(updateCounter));
                    System.out.println(currClusterMetadataSnapshot);
                } else {
                    Logger.info("NO NEW CHANGES IN CLUSTER METADATA DETECTED");
                }
            }

            @Override
            public void onFailure(Throwable t) {
                Logger.error(t);
            }
        }, FluxExecutor.getExecutorService());
    }

    public void close() {
        channel.shutdown();
        FluxExecutor.getSchedulerService().shutdown();
    }

}
