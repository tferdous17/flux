# flux

_Last Updated: 11/26/25_

**Table of Contents**
1. [About](#about)
   1. [Project Goals](#project-goals)
2. [Contributors](#contributors)
3. [Architecture](#architecture)
   1. [Quick Terminology](#quick-terminology)
   2. [High Level Visual Overview (Simplified)](#high-level-visual-overview-simplified)
   3. [Cluster Architecture](#1-cluster-architecture)
   4. [Metadata Infrastructure](#2-metadata-infrastructure)
   5. [Controller Node](#3-controller)
   6. [Broker Node](#4-broker-node)
   7. [Storage Layer](#5-storage-layer)
   8. [Producer Architecture](#6-producer-architecture)
   9. [Consumer Architecture](#7-consumer-architecture)
   10. [Networking (gRPC)](#8-networking-grpc)
4. [References](#references)

# About
**flux** is a (work-in-progress) heavily **Kafka**-inspired distributed message queue platform we are engineering for high throughput, maximal scalability, and fault-tolerance. This project is not meant to be an exhaustive 1:1 clone of Kafka, but implements its core functionality. Built mainly for fun + educational purposes.

## Project Goals
- Understand core distributed systems concepts
- Build a functional end-to-end message broker
- Explore realistic storage internals
- Prioritize conceptual clarity over enterprise-grade complexity
- Learn by **building**

## Non-goals
- Be a production ready, drop-in replacement for Kafka

> [!NOTE]
> **Disclaimer**: this is an amateur distributed systems project so no, our code is not industry standard lol and yes there is undoubtedly room for improvement

# Contributors
[Tasnim Ferdous](https://github.com/tferdous17) (Project Lead)
- Architected the end-to-end infrastructure for Flux, including Broker and Controller node design, cluster membership, and broker registration/decommissioning workflows.
- Developed major subsystems for metadata propagation, topic creation, and topic partition-to-broker assignments for scalability.
- Built admin APIs and the initial producer infrastructure.
- Implemented the underlying log storage layer, including Partition, Log, and LogSegment (immutable, append-only files) with durable disk writes.
- Designed gRPC communication flows between producers, brokers, and consumers, including record offset management.

[Kyoshi Noda](https://github.com/KyoshiNoda)
- Led implementation of Consumer infrastructure and Consumer Group functionality, including group coordination, partition assignment, synchronization, liveness tracking, and more.
- Implemented deterministic round-robin and range assignors for partition assignment.
- Built the foundations for ProducerRecords and RecordAccumulator.
- Implemented Kryo-based serialization for producer messages.

[Kevin Wijaya](https://github.com/icycoldveins)
- Implemented multi-partition support and multi-producer writes to brokers (thread-safe, parallelized).
- Developed sticky assignor for consumer groups and broker liveness tracking.
- Enhanced RecordAccumulator with batching, compression, retry logic, and partition starvation prevention.

[Christopher Maradiaga](https://github.com/maradC)
- Built out the foundation for the Partition class and per-message metadata.

[Carlos Duque](https://github.com/CDDR1)
- Built out the foundation for Consumer records and unit testing.

[Josh Obogbaimhe](https://github.com/J-Obog)
- Contribtued mentorship and PR reviews.


# Architecture
Overview of the end-to-end architecture covering the Producer, Broker, and Consumer.

## Quick Terminology
### Producer
- A **Producer** is any server, process, or application that writes data into the system. For example, YouTube’s upload pipeline might push video chunks into a queue for downstream processing.

### Consumer
- A **Consumer** is a server, process, or application that reads data from the system using a pull-based model. Continuing the example, a video-processing service would consume those queued chunks for encoding or analysis.

### Broker
- A **Broker** is the core component responsible for **storing** and **serving** data. Producers write to brokers and consumers read from them. Internally, a broker is composed of multiple lower-level storage structures such as partitions, logs, and log segments.

### Partition
- A **Partition** is the fundamental storage unit within a broker—an immutable, append-only log. Producers append messages to partitions, and consumers read from them sequentially. Each partition is itself composed of lower-level structures like log segments and index entries.

### Topic
- A **Topic** is a logical grouping of partitions (and by extension, messages). Topics organize data streams by use-case. For example, a topic named "video-post-processing" could contain all partitions related to video chunks awaiting downstream processing.


## High-Level Visual Overview (Simplified)
Here is a high level overview of how Kafka works (which flux is modeled after). This abstracts away aspects such as metadata propagation, controllers, multiple producers/consumers, multiple brokers, multiple partitions, etc, for the sake of simplicity but will get explained later.
<img width="3758" height="1552" alt="image" src="https://github.com/user-attachments/assets/6cf430fa-ea6e-45d5-b51c-9375d476df00" />

This is what flux originally started as—just a single-server model which gradually got built upon until it became fully distributed. 

**The following sections dive deeper into the full architecture of the system—from the initial single-node prototype to the fully distributed design. We walk through each major component of the cluster, explain how they interact, and detail the design decisions behind metadata management, storage, networking, and client behavior.**

> [!NOTE]
> This is how *our* system implements the components. While we tried to mirror Kafka as much as we could, there are some aspects where we deviated for sake of simplicity and quicker development.

For a logical ordering, the sections will be explored in the following manner:
1. Cluster Architecture
2. Metadata Infrastructure
3. Controller Node
4. Broker Node
5. Storage Layer
6. Producer Architecture
7. Consumer Architecture
8. Networking (gRPC)

## 1. Cluster Architecture
A **Cluster** is the logical layer that groups multiple brokers into a single coordinated system. It basically provides a single entry point for producers and consumers while allowing the underlying brokers to scale horizontally.

In Flux, clusters are initialized programmatically through a bootstrap function that accepts a list of broker addresses (e.g., `"localhost:50051, localhost:50052, ...`") and spawns a `Broker` instance for each address. By default in our system, the first address in the bootstrap list is designated as the Controller—the broker responsible for cluster-wide coordination and metadata management (explained more later). Proper Controller election may get implemented later.

Bootstrapping the cluster and **starting** it are separate steps. During startup, the Controller is launched first. The remaining brokers then asynchronously register themselves with the Controller via gRPC requests, during which they initialize their local metadata state. This registration (and future decommissioning) process enables the Controller to maintain an accurate view of the active brokers in the cluster and to perform coordination tasks, such as metadata propagation, efficiently.

You can check out our Cluster code [here](https://github.com/tferdous17/flux/blob/main/src/main/java/server/internal/Cluster.java)

## 2. Metadata Infrastructure
The Metadata API is a core subsystem in flux that all major components depend on, and allows such components to periodically fetch and use the latest metadata within the system.

We implemented Metadata as a singleton object that encapsulates all the logic surrounding metadata and we specifically utilized the Observer design pattern, thus allowing clients (producers/consumers) to listen (`MetadataListener`) to our `Metadata` instance and receive the latest cached snapshot of metadata immediately upon any changes detected in the metadata (which itself periodically updates in scheduled intervals).

To be more specific about *what* metadata we handle, each component related to brokers and clusters have their own dedicated, immutable metadata records (ex: `ControllerMetadata`, `BrokerMetadata`, `PartitionMetadata`, etc) and are all contained within a `ClusterSnapshot` record which is what the Metadata instance periodically fetches from a cluster's `Controller` (pull-based). We set our default fetch interval to be 5 minutes however you may see in our codebase a much shorter interval for testing puroses. In order for the Controller to have the latest metadata for all of it's brokers, each broker in a cluster will periodically send its most up-to-date metadata to the controller via RPCs.

Some important usecases of metadata include:
- Producers must know how many partitions a broker has for the sake of partition selection for a particular message (more detail later)
- Controllers must know the details of their brokers in order to do partition assignment via topic creation
- Topic metadata is necessary so we know its # of partitions and per-partition metadata
- etc..

> [!NOTE]
> The real Kafka implements its metadata subsystem by storing cluster state—including broker membership, topic configurations, and partition assignments—in an internal, replicated **Raft-based KRaft quorum** (or historically ZooKeeper), which is basically its own log.

See the code [here](https://github.com/tferdous17/flux/tree/main/src/main/java/metadata) and check out the below diagram for a visual overview.
<img width="2954" height="984" alt="image" src="https://github.com/user-attachments/assets/15283044-2015-46a1-a039-beabecd6d774" />

## 3. Controller
The Controller broker is a specially designated broker within a cluster that acts like the "leader" of the cluster. It has the same functionality as every other broker, except it comes with additional functionality on top of it to handle special responsibilities which include:
- Maintaining and updating cluster metadata
- Propagating metadata changes to all brokers within the cluster via RPCs
- Handling topic lifestyle (add or remove partitions + distribute them upon receiving topic requests by admin)
- Reassigning partitions for load balancing and scalability
- Monitor broker heartbeats/liveness
- Handle broker registration (new brokers joining) and broker decommissioning (brokers gracefully shutting down)
- ..and much more that we didn't include

As of the latest update, most of the above responsibilities have been implemented in flux and you can check out the [implementations](https://github.com/tferdous17/flux/blob/main/src/main/java/server/internal/Broker.java), and below is a quick diagram.
<img width="900" height="700" alt="image" src="https://github.com/user-attachments/assets/7f5b96c2-6989-415d-b948-f37184174da0" />


## 4. Broker Node
Broker nodes are the primary servers that producers and consumers interact with. Each broker stores partitions (and their replicas), validates incoming writes, and serves read requests.

### Controller Capability
A broker can optionally act as the Controller (the cluster leader). We implemented this through a shared Controller interface that every broker implements. Controller-specific behavior is gated behind an internal flag (isActiveController). When the broker is not the active controller, controller-specific functionality is simply disabled.
 ```java
   public class Broker implements Controller {
       // ...
       private boolean isActiveController = false | true
       // ...
   }
   ```
### General Responsibilities
Write path (Producer → Broker → Storage):
- Validates incoming messages from producers.
- Appends messages to the underlying partitions + delegates further write handling to them.

Read path (Consumer → Broker):
- Serves fetch requests based on the starting offset given by consumers.
- Determines the target partition based on the request, reads from it, and returns the message back to the consumer.

### Networking
Each broker runs an embedded gRPC server to handle external requests. Brokers can also act as gRPC clients when sending metadata updates to the Controller node or communicating with other brokers.
For simplicity, broker ports in the current implementation default to `:50051` and increment sequentially for additional nodes. All broker servers support graceful shutdown.

## 5. Storage Layer
Flux’s storage layer is built as a stack of progressively lower-level components, moving closer to disk as you go down: <br>
**Broker → Partition → Log → LogSegment & IndexEntries**

### Partition
A Partition is the fundamental append-only queue in Flux (mirroring Kafka’s design). Each partition owns a single Log, which is internally split into multiple LogSegments stored on disk. Producers append to partitions; consumers fetch from them based on offsets. 

### Log
A Log represents the full, continuous record stream for a single partition.
- Internally, it is composed of multiple ordered LogSegments, each representing a chunk of the partition’s data on disk.
- The Log acts as a segment manager: it always writes to the active segment.
- When the active segment reaches its configured size limit, it becomes immutable and a new segment is created.
- Because log files are immutable once closed, this segmentation is essential for retention, cleanup, and efficient disk writes.

### LogSegment
A LogSegment is the core on-disk storage unit (aka the log file). Each segment:
- Stores records sequentially (usually grouped into batches).
- Tracks metadata such as start/end offsets, byte thresholds, current write position, and references to its log file and index file.
- Becomes read-only once it hits its byte threshold.

Log segments maintain an internal buffer of incoming writes (with a configurable byte threshold), which allows us to **batch** writes together and periodically flush to disk—ultimately reducing the number of disk writes we have to do which is crucial for performance. At the same time, we populate index entries per write which gets flushed to disk at the same time as the log segment.

### IndexEntries
Index entries is essentially a map containing a bunch of `message offset → byte offset` pairs on disk. These files are important for faster lookup on disk as we can lookup a message and immediately find its location in a given file via the associated byte offset which saves us from doing costly full table scans. Index writes are also buffered and flushed in sync with the corresponding segment flush, keeping the log and its index consistent.

## 6. Producer Architecture
As mentioned before, Producers are applications writing data to our servers. In flux, we implemented producers directly as part of the codebase rather than as standalone external clients, since Flux is not (currently) intended to be a production-ready, externally consumed system.

We define a `Producer` as a simple interface, with our `FluxProducer` class providing the concrete implementation:
```java
public interface Producer<K, V> {
    void send(ProducerRecord<K,V> record) throws IOException;
    void close();
}
```
where our main focus is on the `send(...)` call, which performs partition selection, serialization, batching, and finally ships the record to the broker via gRPC.

### ProducerRecord
This is a key/value pair to be sent to the broker, which consists of a topic name to which the record is being sent, an optional partition number, a timestamp, and an optional key and required value. These fields are also used to help determine a target partition for a given record, explained further below.

Messages are serialized into a compact binary format using Kryo, and we attach additional metadata needed by other internal subsystems/functions before sending it to the broker. Records are only deserialized back into their original form on the consumer side when constructing consumer records. You may see some intermediary representations of messages passed around in our codebase in the overall write and read flows.

Partition selection for any given record happens **before** any data is sent to the broker. This is why the Metadata subsystem is crucial: producers must know the number of partitions for a given topic in order to correctly load-balance traffic.

### Partition Selection
We built a small utility class, [PartitionSelector](https://github.com/tferdous17/flux/blob/main/src/main/java/commons/utils/PartitionSelector.java), to encapsulate the partition-selection logic. The priority order mirrors Kafka's behavior:
   1. If a partition # is passed in to the record already, this takes top priority.
   2. If invalid partition number (out of range/null), attempt key-based hashing.
   3. If above two methods didn't work, default to round-robin.
   4. If a topic is also passed in, this just narrows down which partitions we can select for the above operations.

Other than simply sending records to our broker, we also implement a buffering layer before we actually ship off our records to the broker. This buffering layer is known as the `RecordAccumulator`.

### RecordAccumulator
Before records are sent to brokers over the wire, they are buffered/staged in a `RecordAccumulator`, which batches messages and periodically drains them to the appropriate broker. Internally, the accumulator manages multiple `RecordBatch` instances, each containing many producer records. Batches have size limits, support compression (gzip, snappy, etc.), and track their own metadata.

The accumulator mirrors several of Kafka’s buffering behaviors:
- Maintaining per-topic, per-partition batch queues
- Tracking in-flight batches on a per-partition basis
- Draining ready batches per broker (using per-broker round-robin to ensure fairness and even load distribution)

Flux supports **multiple concurrent producers**, allowing many producers to append to the same partition simultaneously while still guaranteeing sequential, ordered writes within that partition. Producers can write in parallel without compromising correctness or ordering guarantees.
- This concurrency control is managed by our [PartitionWriteManager](https://github.com/tferdous17/flux/blob/main/src/main/java/commons/utils/PartitionWriteManager.java), which ensures thread-safe writes through a combination of Java’s `ReentrantLock` and concurrent data structures. The manager serializes writes at the **partition level** while allowing for parallelism across different partitions and producers.

<img width="3836" height="1480" alt="image" src="https://github.com/user-attachments/assets/1c1b9b68-afd9-4457-b043-f7dfeb5d0bee" />

## 7. Consumer Architecture
Similar to producers, consumers are implemented directly within the Flux codebase and exposed as a simple interface:
```java
public interface Consumer {
    void subscribe(Collection<String> topics);
    void unsubscribe();
    PollResult poll(Duration timeout);
    void commitOffsets();
}
```
Consumers do more than simply read messages— they maintain offset state, participate in consumer groups, and perform various coordination steps with the cluster.

### ConsumerRecord
A `ConsumerRecord` is the key/value pair fetched from a broker and returned to the application. It mirrors the structure of a ProducerRecord but includes an additional offset field: the logical index of the message within the partition. Offsets allow the consumer to know exactly where it left off and where to resume from on subsequent polls.

### Consuming Records (Polling)
Polling is the core mechanism for retrieving records from a broker. The poll(Duration timeout) method repeatedly issues FetchMessageRequests to brokers until either:
- messages are returned, or
- the poll timeout expires.

Each fetch returns a Message object (an internal, intermediary representation we have), which is then converted into a `ConsumerRecord` before being returned to the consumer that is polling.

Consumers always poll starting from a specific **offset**, which they maintain themselves. This offset advances as messages are consumed and can be persisted via `commitOffsets()` so that the consumer can resume correctly after restarts or failures. NOTE: The commit offsets functionality is still in the works

For a **simplified** view of reading from a partition, we visualized it like so (note we're abstracting some layers here):
<img width="1500" height="640" alt="image" src="https://github.com/user-attachments/assets/b174bb60-9b1f-4ae7-b5ae-b82f081c0c81" />


### Consumer Groups
Consumer groups allow multiple consumers to share the work of reading from a **subscribed topic** while ensuring each partition is consumed by **at most** one consumer at a time. This enables horizontal scaling of consumption without duplicating reads. We implement this feature in flux while replicating Kafka's original design as described below.

When a consumer joins a group, it registers with the `GroupCoordinator`, which tracks active and newly joined members and performs partition assignment. Flux distributes partitions evenly across consumers (e.g., round-robin, range assignment, or sticky), and each consumer receives an explicit list of partitions it is allowed to poll.

Consumers periodically heartbeat to the `GroupCoordinator`. If a consumer fails or new members join the group, the `GroupCoordinator` triggers a rebalance—recomputing partition ownership and redistributing partitions among the remaining consumers. During a rebalance, polling is temporarily paused to avoid conflicts.

You can check out our code for implementing consumer groups [here](https://github.com/tferdous17/flux/tree/main/src/main/java/consumer) and how we actually performed the partition assignment to consumers, handling coordination, and `JoinGroup` requests from other consumers.

## 8. Networking (gRPC)
Flux uses gRPC as the communication layer between producers, brokers, consumers, and controller nodes. While Apache Kafka relies on a custom high-performance TCP protocol, we opted for gRPC + Protocol Buffers as we saw implementing a custom TCP protocol to be too overkill for a personal project.

**Why not REST?** <br>
REST can work, but it comes with several downsides for a high-throughput messaging system:
- Text-based serialization (JSON) is slower and larger than Protocol Buffers’ compact binary format.
- HTTP/1.1 limitations (no multiplexing, higher overhead per request) introduce unnecessary latency. gRPC, built on HTTP/2, supports streaming, multiplexing, and efficient connection reuse—features.
- As of currently we don't intend for this to be a production-ready, usable system so using gRPC allowed us to be a little more "programmatic" with our requests as that's the nature of gRPC (the network calls look like invoking functions)

In short: gRPC gives us significantly better performance characteristics and a development experience that’s still approachable, without requiring us to build a full protocol ourselves.

Flux defines multiple gRPC [services](https://github.com/tferdous17/flux/tree/main/src/main/java/grpc/services) to support the distributed system’s functionality, including:
- Message produce/consume
- Metadata
- Consumer group coordination
- Liveness tracking
- etc..

Below is a simple diagram from the earlier stages of our project that just shows the networking flow despite being a bit outdated:
<img width="950" height="572" alt="image" src="https://github.com/user-attachments/assets/e9cf3886-a57a-45d2-8f8a-9934c9a97bd2" />

# References
### Resources we used throughout the development process for understanding Kafka's internals and architecture
- https://kafka.apache.org/documentation/
- https://grpc.io/docs/languages/java/basics/
- https://jaceklaskowski.gitbooks.io/apache-kafka/content/
- https://www.hellointerview.com/learn/system-design/deep-dives/kafka
- https://github.com/apache/kafka


# INTERNAL DOCUMENTATION

## gRPC Quick Start
The `pom.xml` file should already come with the proper dependencies, but if not please make sure to include the following:
```html
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty-shaded</artifactId>
    <version>1.71.0</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>1.71.0</version>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-stub</artifactId>
    <version>1.71.0</version>
</dependency>
<dependency> <!— necessary for Java 9+ —>
    <groupId>org.apache.tomcat</groupId>
    <artifactId>annotations-api</artifactId>
    <version>6.0.53</version>
    <scope>provided</scope>
</dependency>
```

Additionally, include the following below the `<dependencies>` section if missing:
```html
<build>
    <extensions>
        <extension>
            <groupId>kr.motd.maven</groupId>
            <artifactId>os-maven-plugin</artifactId>
            <version>1.7.1</version>
        </extension>
    </extensions>
    <plugins>
        <plugin>
            <groupId>org.xolstice.maven.plugins</groupId>
            <artifactId>protobuf-maven-plugin</artifactId>
            <version>0.6.1</version>
            <configuration>
                <protoSourceRoot>${project.basedir}/src/main/proto</protoSourceRoot>
                <protocArtifact>com.google.protobuf:protoc:3.25.5:exe:${os.detected.classifier}</protocArtifact>
                <pluginId>grpc-java</pluginId>
                <pluginArtifact>io.grpc:protoc-gen-grpc-java:1.71.0:exe:${os.detected.classifier}</pluginArtifact>
            </configuration>
            <executions>
                <execution>
                    <goals>
                        <goal>compile</goal>
                        <goal>compile-custom</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```
After having the above in the pom.xml file, run the following command in your terminal:
`mvn clean compile`

The mvn command will **autogenerate** all the protobuf and gRPC files you need for the system to function under the `target/` folder.

### Side notes:
* If you don't have maven installed on your system, install it and add it to your PATH (google how to do it) so you can run the `mvn` command
* If you have mac, theres a chance the pom.xml will fetch the **wrong** `protoc-gen-grpc-java` executable from Maven. This is because Maven purposely
 has the Window's executable named as the arm64 (mac), because there is no native arm64 support yet for some reason via Maven (very very very very very stupid thing on Maven's part)
  * In this case, you might need to switch to a Windows PC to get everything running or try homebrew's gRPC package(s).

—-

## Mac/Apple Silicon Setup Issues & Solution

**Why this workaround?**
On Mac (especially Apple Silicon/ARM), Maven's gRPC plugin often fetches an incompatible or Windows binary for `protoc-gen-grpc-java`, causing gRPC Java code generation to fail. This is due to a lack of native ARM support in the Maven plugin distribution. The workaround is to use Homebrew to install the correct `protoc`, `grpc`, and `protoc-gen-grpc-java` tools.

1. **Install protoc, grpc, and the gRPC Java plugin via Homebrew:**
   ```sh
   brew install protobuf grpc protoc-gen-grpc-java
   ```

2. **Compile your project:**
   ```sh
   mvn compile
   ```

**Troubleshooting: Missing proto/gRPC Java classes**
If you see errors like `cannot find symbol` for classes such as `Message`, `FetchMessageRequest`, `PublishDataToBrokerRequest`, etc., it means the Java files for your protobuf messages and gRPC services were not generated or are not in the expected location.

**How to fix:**
1. Regenerate the Java and gRPC files manually:
   ```sh
   protoc —java_out=target/generated-sources/protobuf/java —proto_path=src/main/proto src/main/proto/*.proto
   protoc —grpc-java_out=target/generated-sources/protobuf/grpc-java —proto_path=src/main/proto src/main/proto/*.proto
   ```
2. Re-run Maven:
   ```sh
   mvn compile
   ```
3. If you still get errors:
   - Check that the generated `.java` files exist in `target/generated-sources/protobuf/java/proto/` and `target/generated-sources/protobuf/grpc-java/proto/`.
   - Make sure your IDE recognizes these as source folders (sometimes you need to mark them as such).

  ```

