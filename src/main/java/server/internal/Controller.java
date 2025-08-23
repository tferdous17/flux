package server.internal;

import proto.Topic;

import java.io.IOException;
import java.util.Collection;

/**
 * The Controller is a special broker within a cluster that acts as the "leader" of the cluster.
 * It is responsible for handling special tasks such as managing/propagating metadata, topic lifestyle,
 * distributing partitions, broker liveness, registration, and more.
 *
 * All brokers will implement this interface, however the extra functionality given by this interface
 * will not be "activated" unless that broker is designated as the controller.
 */
public interface Controller {
    void createTopics(Collection<Topic> topics) throws IOException;
    void registerBroker();
    void decommissionBroker();
    void processBrokerHeartbeat(); // TODO: Update params when broker heartbeat ticket is being worked on
}
