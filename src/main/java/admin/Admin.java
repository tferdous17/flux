package admin;

public interface Admin {
    void createTopic(String name, int numPartitions);
    void increasePartitions(String topicName, int partitionCount);
}
