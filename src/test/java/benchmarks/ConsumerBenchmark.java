package benchmarks;

import server.internal.Broker;
import consumer.FluxConsumer;
import consumer.PollResult;
import grpc.BrokerServer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import producer.FluxProducer;
import producer.ProducerRecord;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * consumer benchmark is tweaking out
 * when running producer benchmark just comment out this whole file or delete it otherwise the benchmark
 * will not run properly
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class ConsumerBenchmark {
    Broker broker;
    BrokerServer server;
    FluxProducer<String, String> producer;
    FluxConsumer<String, String> consumer = new FluxConsumer<>();

    private String randStringGen() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 50; i++) {
            sb.append(chars.charAt((int) (Math.random() * chars.length())));
        }
        return sb.toString();
    }

    @Setup(Level.Trial)
    public void setup() {
        try {
            broker = new Broker();
            server = new BrokerServer(broker);
            Thread serverThread = new Thread(() -> {
                try {
                    server.start(8080);
                    server.blockUntilShutdown();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start server: " + e.getMessage(), e);
                }
            });

            serverThread.start();

            producer = new FluxProducer<>();

            for (int i = 0; i < 10_000; i++) {
                String value = randStringGen();
                ProducerRecord<String, String> record = new ProducerRecord<>("topic" + (i % 10), value);
                producer.send(record);
            }
            producer.forceFlush();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Benchmark
    public void consumeRecordsFromBroker(Blackhole blackhole) {
        while (true) {
            PollResult result = consumer.poll(Duration.ofMillis(1000));
            blackhole.consume(result);
            if (!result.shouldContinuePolling()) {
                return;
            }
//            int consumedCount = result.records().size();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            if (server != null) {
                server.stop();
            }
        } catch (Exception e) {
            System.err.println("Teardown error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
