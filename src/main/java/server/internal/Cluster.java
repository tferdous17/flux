package server.internal;

import commons.FluxExecutor;
import grpc.BrokerServer;
import metadata.Metadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Cluster {
    private String clusterId; // ex: CLUSTER-1
    private List<Broker> nodes;
    private Broker controllerNode;

    public Cluster(String clusterId) {
        this.clusterId = clusterId;
        this.nodes = new ArrayList<>();
    }

    // Initializes all the brokers to be in this cluster, but does not yet start them up.
    public void bootstrapCluster(Set<InetSocketAddress> bootstrapServerAddrs) throws IOException {
        // for each address, create a broker @ that particular address
        for (InetSocketAddress addr : bootstrapServerAddrs) {
            Broker broker = new Broker("BROKER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()), addr.getHostName(), addr.getPort());
            nodes.add(broker);
        }
        controllerNode = nodes.get(0);
    }

    // Fire up each server, ready for requests.
    public void startCluster() {
        for (Broker b : nodes) {
            // Start up each broker in its own thread
            FluxExecutor.getExecutorService().submit(() -> {
                BrokerServer server = new BrokerServer(b);
                try {
                    server.start(b.getPort());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
