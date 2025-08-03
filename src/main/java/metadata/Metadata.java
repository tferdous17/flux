package metadata;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import commons.FluxExecutor;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.tinylog.Logger;
import proto.FetchBrokerMetadataRequest;
import proto.FetchBrokerMetadataResponse;
import proto.MetadataServiceGrpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Encapsulates the logic around metadata
 *
 * Utilizes the Observer pattern to notify clients (producers/consumers) of any changes in metadata.
 */
public class Metadata {
    private int refreshIntervalSec;
    private int updateCounter;
    private AtomicReference<BrokerMetadataSnapshot> currBrokerMetadataSnapshot; // Cached metadata snapshot
    private ManagedChannel channel;
    private final MetadataServiceGrpc.MetadataServiceFutureStub metadataFutureStub;
    private List<MetadataListener> listeners = new ArrayList<>();

    public Metadata(int refreshIntervalSec) {
        this.refreshIntervalSec = refreshIntervalSec;
        updateCounter = 0;
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        metadataFutureStub = MetadataServiceGrpc.newFutureStub(channel);

        currBrokerMetadataSnapshot = new AtomicReference<>(initialMetadataFetch()); // This is blocking

        System.out.println(currBrokerMetadataSnapshot);
        FluxExecutor
                .getSchedulerService()
                .scheduleWithFixedDelay(this::updateMetadata, refreshIntervalSec, refreshIntervalSec, TimeUnit.SECONDS);
    }

    public Metadata() {
        this(300); // default = 5 minutes
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
            Logger.info("LISTENER ADDED");
        }
    }

    public void notifyListeners() {
        Logger.info("METADATA CHANGE DETECTED -- NOTIFYING + UPDATING ALL LISTENERS");
        synchronized (listeners) {
            for (MetadataListener listener : listeners) {
                listener.onUpdate(currBrokerMetadataSnapshot);
            }
        }
    }

    // Get the current metadata
    public AtomicReference<BrokerMetadataSnapshot> getBrokerMetadataSnapshot() {
        return currBrokerMetadataSnapshot;
    }

    private BrokerMetadataSnapshot initialMetadataFetch() {
        FetchBrokerMetadataRequest request = FetchBrokerMetadataRequest.newBuilder().build();
        try {
            FetchBrokerMetadataResponse response = metadataFutureStub
                    .fetchBrokerMetadata(request)
                    .get();

            return new BrokerMetadataSnapshot(
                    response.getBrokerId(),
                    response.getHost(),
                    response.getPortNumber(),
                    response.getNumPartitions()
            );
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateMetadata() {
        Logger.info("REFRESHING METADATA SNAPSHOT");

        FetchBrokerMetadataRequest request = FetchBrokerMetadataRequest.newBuilder().build();
        ListenableFuture<FetchBrokerMetadataResponse> future = metadataFutureStub.fetchBrokerMetadata(request);

        Futures.addCallback(future, new FutureCallback<FetchBrokerMetadataResponse>() {
            @Override
            public void onSuccess(FetchBrokerMetadataResponse response) {
                currBrokerMetadataSnapshot.set(new BrokerMetadataSnapshot(
                        response.getBrokerId(),
                        response.getHost(),
                        response.getPortNumber(),
                        response.getNumPartitions()
                ));
                updateCounter++;
                notifyListeners();
                Logger.info("SUCCESSFULLY REFRESHED CACHED BROKER METADATA, UPDATE COUNT = %d".formatted(updateCounter));
            }

            @Override
            public void onFailure(Throwable t) {
                Logger.error(t);
            }

        }, FluxExecutor.getExecutorService());
    }


}
