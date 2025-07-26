package exceptions;

public class InvalidTopicException extends RuntimeException {
    public InvalidTopicException(String topicName) {
        super("Topic \"%s\" does not exist. Please check your spelling or create the desired topic first.".formatted(topicName));
    }
}
