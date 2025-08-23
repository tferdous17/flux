package server.internal;

import commons.FluxExecutor;
import grpc.BrokerServer;
import metadata.Metadata;
import org.tinylog.Logger;

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
    public void bootstrapCluster(List<InetSocketAddress> bootstrapServerAddrs) throws IOException {
        // for each address, create a broker @ that particular address
        for (int i = 0; i < bootstrapServerAddrs.size(); i++) {
            Broker broker = new Broker(
                    "BROKER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()),
                    bootstrapServerAddrs.get(i).getHostName(),
                    bootstrapServerAddrs.get(i).getPort()
            );
            nodes.add(broker);
        }

        // Pick first node by default since we currently don't have a consensus algo implemented rn
        controllerNode = nodes.get(0);
        controllerNode.setIsActiveController(true);
        String controllerEndpoint = "%s:%d".formatted(bootstrapServerAddrs.get(0).getHostName(), bootstrapServerAddrs.get(0).getPort());
        controllerNode.setControllerEndpoint(controllerEndpoint);

        if (nodes.size() > 1) {
            // All other nodes must then store the controller node's endpoint in order to make further requests,
            // i.e., broker registration, heartbeats, etc
            List<Broker> followerNodes = nodes.subList(1, nodes.size());
            for (Broker node : followerNodes) {
                node.setControllerEndpoint(controllerNode.getControllerEndpoint());
            }
        }
    }

    // Fire up each server, ready for requests.
    public void startCluster() {
        for (Broker b : nodes) {
            // Start up each broker in its own thread
            FluxExecutor.getExecutorService().submit(() -> {
                BrokerServer server = new BrokerServer(b);
                try {
                    server.start(b.getPort());
                    // Once each server is started, it will immediately make a BrokerRegistrationRequest to the controller
                    if (!b.isActiveController()) {
                        b.registerBroker();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
