package server.internal;

/**
 * Functional interface used to shutdown the Broker server in cases of decommission requests.
 * Must match name of the stop method in BrokerServer in order to be implemented via lambda/method reference
 */
public interface ShutdownCallback {
    void stop() throws InterruptedException;
}
