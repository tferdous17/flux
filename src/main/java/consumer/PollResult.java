package consumer;

import java.util.List;

public record PollResult(List<ConsumerRecord<String, String>> records, boolean shouldContinuePolling) {}
