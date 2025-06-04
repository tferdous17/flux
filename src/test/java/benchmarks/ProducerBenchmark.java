package benchmarks;

import broker.Broker;
import grpc.BrokerServer;
import org.openjdk.jmh.annotations.*;
import producer.FluxProducer;
import producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class ProducerBenchmark {
    Broker broker;
    BrokerServer server;
    FluxProducer<String, String> producer;
    List<ProducerRecord<String, String>> records = new ArrayList<>();

    /**
     * Setup method to initialize data for the benchmark.
     */
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

            producer = new FluxProducer<>(300);
            records = new ArrayList<>();

            for (int i = 0; i < 10_000; i++) {
                String value = randStringGen();
                ProducerRecord<String, String> record = new ProducerRecord<>("topic" + (i % 10), value);
                records.add(record);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 6/2/2025
     * multiply score by 10,000 i think
     * Benchmark                               Mode  Cnt  Score   Error  Units
     * ProducerBenchmark.sendRecordsToBroker  thrpt   25  2.263 � 0.076  ops/s
     *
     * translates to 2480 records per second WITHOUT any batching
     */
    @Benchmark
    public void sendRecordsToBroker() {
        try {
            for (ProducerRecord<String, String> record : records) {
                producer.send(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @TearDown(Level.Trial)
    public void tearDown() {
        try {
            if (server != null) {
                server.stop();
            }
            records.clear();
        } catch (Exception e) {
            System.err.println("Teardown error: " + e.getMessage());
        }
    }

    /**

     /**
     * Main method to run the benchmark.
     *
     * @param args Command line arguments
     * @throws Exception If an error occurs during benchmark execution
     */
    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
