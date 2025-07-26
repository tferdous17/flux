package admin;

// Does not represent a formal Topic object, this is just a plain object used by the Admin client when
// building a createTopics request. Actual topic is validated + constructed by the Controller broker.
public record NewTopic(String name, int numPartitions, int replicationFactor) {
}
