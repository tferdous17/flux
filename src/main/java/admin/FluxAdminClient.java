package admin;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import metadata.Metadata;
import org.tinylog.Logger;
import proto.CreateTopicsRequest;
import proto.CreateTopicsResult;
import proto.CreateTopicsServiceGrpc;
import server.internal.Cluster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * FluxAdmin is a Singleton class to represent a single Admin client.
 * The admin client is responsible for creating topics, managing partitions per topic,
 * and much more.
 *
 * Note: The FluxAdminClient can also programmatically start up a cluster of Brokers upon initialization, unlike Kafka.
 */
public class FluxAdminClient implements Admin {
    private static FluxAdminClient instance = null;
    private Set<InetSocketAddress> bootstrapServerAddrs; // addresses of initial brokers (localhost:50051..etc)

    private final CreateTopicsServiceGrpc.CreateTopicsServiceBlockingStub blockingStub;
    private ManagedChannel channel; // channel to the broker server

    private FluxAdminClient(Properties props) {
        bootstrapServerAddrs = new HashSet<>();
        if (props.containsKey("bootstrap.servers")) {
            String[] addrs = props.getProperty("bootstrap.servers").split(",");
            for (String addr : addrs) {
                String[] tokens = addr.split(":");
                bootstrapServerAddrs.add(
                        new InetSocketAddress(tokens[0], Integer.parseInt(tokens[1]))
                );
            }
        }

        // If bootstrapServerAddrs is NOT empty, we'll go ahead and start a cluster automatically.
        if (!bootstrapServerAddrs.isEmpty()) {
            try {
                initAndStartBootstrapCluster();
            } catch (IOException e) {
                Logger.error("Could not start up bootstrap cluster: " + e.getMessage());
            }
        }

        // we send requests to the controller broker but by default we'll send all reqs to the first broker addr
        // if it's not the controller, reroute it somehow (later problem)

        // InetSocketAddress full address has an extra bit of info we don't need, so manually convert to host:port
        String addr = "%s:%d".formatted(
                bootstrapServerAddrs.stream().findFirst().get().getHostName(),
                bootstrapServerAddrs.stream().findFirst().get().getPort()
        );
        channel = Grpc.newChannelBuilder(addr, InsecureChannelCredentials.create()).build();
        blockingStub = CreateTopicsServiceGrpc.newBlockingStub(channel);
    }

    // Can create Admin with a set of starting servers.
    public static Admin create(Properties props) {
        if (instance == null) {
            Logger.info("Creating new Admin client");
            instance = new FluxAdminClient(props);
        } else {
            Logger.warn("Admin client already exists");
        }
        return instance;
    }

    // NOTE: We have 3 ways to represent a topic, all used for different purposes.
    // proto.Topic -> used to build proto messages
    // admin.NewTopic -> plain object to represent initial metadata when building our gRPC request
    // commons.FluxTopic -> the *actual* topic within our system. contains partitions and other info
    @Override
    public void createTopics(Collection<NewTopic> topics) {
        List<proto.Topic> newTopicsList = new ArrayList<>();
        for (NewTopic topic : topics) {
            proto.Topic newTopic = proto.Topic
                    .newBuilder()
                    .setTopicName(topic.name())
                    .setNumPartitions(topic.numPartitions())
                    .setReplicationFactor(topic.replicationFactor())
                    .build();
            newTopicsList.add(newTopic);
        }

        CreateTopicsRequest request = CreateTopicsRequest
                .newBuilder()
                .addAllTopics(newTopicsList)
                .build();

        CreateTopicsResult response;
        try {
            response = blockingStub.createTopics(request);
            System.out.println(response.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initAndStartBootstrapCluster() throws IOException {
        // Uses the bootstrap server addresses we were given
        Cluster cluster = new Cluster("CLUSTER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()));
        cluster.bootstrapCluster(this.bootstrapServerAddrs);
        cluster.startCluster();
    }

    public void initAndStartNewCluster(Set<InetSocketAddress> bootstrapBrokerServerAddrs) throws IOException {
        Cluster cluster = new Cluster("CLUSTER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()));
        cluster.bootstrapCluster(bootstrapBrokerServerAddrs);
        cluster.startCluster();
    }
}