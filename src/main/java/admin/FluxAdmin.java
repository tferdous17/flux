package admin;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.tinylog.Logger;
import proto.CreateTopicsRequest;
import proto.CreateTopicsResult;
import proto.CreateTopicsServiceGrpc;
import proto.Topic;

import java.util.*;

/**
 * FluxAdmin is a Singleton class to represent a single Admin client.
 * The admin client is responsible for creating topics, managing partitions per topic,
 * and much more.
 */
public class FluxAdmin implements Admin {
    private static FluxAdmin instance = null;
    private List<String> bootstrapServerAddrs; // addresses of initial brokers (localhost:50051..etc)

    private final CreateTopicsServiceGrpc.CreateTopicsServiceBlockingStub blockingStub;
    private ManagedChannel channel;

    private FluxAdmin(List<String> bootstrapServerAddrs) {
        this.bootstrapServerAddrs = bootstrapServerAddrs;
        // we send requests to the controller broker but by default we'll send all reqs to the first broker addr
        // if it's not the controller, reroute it somehow (later problem)
        channel = Grpc.newChannelBuilder(bootstrapServerAddrs.get(0), InsecureChannelCredentials.create()).build();
        blockingStub = CreateTopicsServiceGrpc.newBlockingStub(channel);
    }

    public static Admin create(List<String> bootstrapServerAddrs) {
        if (instance == null) {
            Logger.info("Creating new Admin client");
            instance = new FluxAdmin(bootstrapServerAddrs);
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
            return;
        }
    }

}
