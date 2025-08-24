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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
        // Must fire up Controller node first so that it's ready to accept requests immediately
        CountDownLatch latch = new CountDownLatch(1);
        FluxExecutor.getExecutorService().submit(() -> {
            BrokerServer controllerServer = new BrokerServer(controllerNode);
            try {
                controllerServer.start(controllerNode.getPort());
                latch.countDown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            // The thread will block until our latch gets counted down to 0 (which only happens after the controller starts up).
            // By doing this, we can ensure the controller is ready before sending any requests.
            if (latch.await(10, TimeUnit.SECONDS)) {
                for (Broker b : nodes) {
                    if (!b.isActiveController()) {
                        // Start up each broker in its own thread
                        FluxExecutor.getExecutorService().submit(() -> {
                            BrokerServer server = new BrokerServer(b);
                            try {
                                server.start(b.getPort());
                                // Once each server is started, it will immediately make a BrokerRegistrationRequest to the controller
                                b.registerBroker();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
