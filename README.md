# flux

_Last Updated: 11/22/25_

**Table of Contents**
1. [About](#about)
2. [Contributors](#contributors)
3. [Architecture](#architecture)
   1. [Quick Terminology](#quick-terminology)
   2. [High Level Visual Overview (Simplified)](#high-level-visual-overview-simplified)
   3. [Cluster Architecture](#1-cluster-architecture)
   4. [Controller Node](#2-controller)
   5. [Metadata Infrastructure](#3-metadata-infrastructure)
5. [References](#references)
6. [Internal Documentation](#internal-documentation)

# About
flux is a heavily Kafka-inspired distributed message queue platform engineered for high throughput, maximal scalability, and fault-tolerance. This project is not meant to be an exhaustive 1:1 clone of Kafka, but implements its core functionality. Built mainly for fun + educational purposes.

> [!NOTE]
> **Disclaimer**: this is an amateur distributed systems project so no, our code is not industry standard lol and yes there is undoubtedly room for improvement

# Contributors
[Tasnim Ferdous](https://github.com/tferdous17)
- Project Lead, architected the end-to-end infrastructure for flux. Designed and implemented the Broker, controller-node infrastructure, cluster membership, and broker registration/decommissioning workflows; developed major subsystems such as metadata propagation, topic creation (+ partition-to-broker assignments for scalability), admin APIs, and initial producer infrastructure. Implemented underlying log-storage components including Partition, Log, LogSegment (immutable, append-only log file), and durable disk writes. Designed gRPC communication flows between producers, brokers, and consumers, along with record offset-management.

[Kyoshi Noda](https://github.com/KyoshiNoda)
- Led implementation of the entire Consumer infrastructure & Consumer Group functionality including group coordination, partition-to-consumer assignments, synchronization, and liveness tracking. Additionally implemented deterministic round robin and range assignors for partition assignments. Laid the groundwork for ProducerRecords + RecordAccumulator, and implemented Kryo-based serialization for incoming producer messages.

[Kevin Wijaya](https://github.com/icycoldveins)
- Led implementation of multi-partition support, multi-producer writing to brokers (parallelized & thread-safe), broker liveness tracking, sticky assignor implementation (consumer groups), and implemented batching, compression, retry logic, and partition starvation prevention in RecordAccumulator.

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
- A Producer is simply a server/process/application that **sends** data into our system. An example producer can be Youtube, pushing videos/video chunks to a queue in order for it to be consumed later by a post-processing service.

### Consumer
- A Consumer is simply a server/process/application that **reads** data from our system (via a pull-based model). An example would be the aforementioned post-processing service (or worker nodes) which consume from the queue.

### Broker
- The Broker is the core storage mechanism/queue that holds data. This is what producers and consumers are communicating with to send/read data, and is really a bunch of subcomponents built on top of each other.

### Partition
- Partitions are the underlying storage mechanism of Brokers (our "queue") and are really just immutable, append-only log files. This is where data really gets produced to/consumed from. Partitions are technically made up of other subcomponents such as logs and log segments.

### Topic
- Topics are just logical groupings of partitions (and by extension, messages). To continue with the earlier example, a potentional topic could be "Video Post Processing" and _only_ contains partitions related to storing videos to be consumed later by a post-processing service.


## High-Level Visual Overview (Simplified)
Here is a high level overview of how Kafka works (which flux is modeled after). This abstracts away aspects such as metadata propagation, controllers, multiple producers/consumers, multiple brokers, multiple partitions, etc, for the sake of simplicity but will get explained later.
<img width="3758" height="1552" alt="image" src="https://github.com/user-attachments/assets/6cf430fa-ea6e-45d5-b51c-9375d476df00" />

This is what flux originally started as--just a single-server model which gradually got built upon until it became fully distributed. 

**The following sections dive deeper into the full architecture of the system—from the initial single-node prototype to the fully distributed design. We walk through each major component of the cluster, explain how they interact, and detail the design decisions behind metadata management, storage, networking, and client behavior.**

> [!NOTE]
> This is how *our* system implements the components. While we tried to mirror Kafka as much as we could, there are some aspects where we deviated for sake of simplicity and quicker development.

For a logical ordering, the sections will be explored in the following manner:
1. Cluster Architecture
2. Controller Node
3. Metadata Infrastructure
4. Broker Node
5. Storage Layer
6. Networking (gRPC)
7. Producer Architecture
8. Consumer Architecture

## 1. Cluster Architecture
A cluster is thin layer that groups of Brokers together, and in a way serves as a single point of entry to a set of Brokers. 

In flux, we initialize a cluster programatically via a bootstrap function that takes in a set of server addresses as a paramater (ex: `"localhost:50051, localhost:50052, ..."`) and creates a `Broker` instance hosted on the given addresses. By default, our system picks the first address given to us and designates the corresponding Broker hosted on that address as the active `Controller` for the cluster (more detail later).

Starting the cluster is separate from bootstrapping it--which we also do programatically. Upon starting a cluster, we first fire up the Controller node and then have all the other brokers in this cluster asynchronously register themselves with the controller via network requests and initialize their metadata as necessary. Broker registration (and decomissioning) is necessary as this is how the controller can track the active brokers its responsible for within a cluster and can perform certain actions such as metadata propagation more easily.

You can check out our Cluster code [here](https://github.com/tferdous17/flux/blob/main/src/main/java/server/internal/Cluster.java)

## 2. Controller
The Controller broker is a specially designated broker within a cluster that acts like the "leader" of the cluster. It has the same functionality as every other broker, except it comes with additional functionality on top of it to handle special responsibilities which include:
- Maintaining and updating cluster metadata- Propagating metadata changes to all brokers within the cluster via RPCs
- Handling topic lifestyle (add or remove partitions + distribute them upon receiving topic requests by admin)
- Reassigning partitions for load balancing and scalability
- Monitor broker heartbeats/liveness
- Handle broker registration (new brokers joining) and broker decommissioning (brokers gracefully shutting down)
- ..and much more that we didn't include

As of the latest update, most of the above responsibilities have been implemented in flux and you can check out the [implementations](https://github.com/tferdous17/flux/blob/main/src/main/java/server/internal/Broker.java), and below is a quick diagram.
<img width="1200" height="700" alt="image" src="https://github.com/user-attachments/assets/7f5b96c2-6989-415d-b948-f37184174da0" />

## 3. Metadata Infrastructure
The Metadata API is a core subsystem in flux that all major components depend on, and allows such components to periodically fetch and use the latest metadata within the system.

We implemented Metadata as a singleton object that encapsulates all the logic surrounding metadata and we specifically utilized the Observer design pattern, thus allowing clients (producers/consumers) to listen (`MetadataListener`) to our `Metadata` instance and receive the latest cached snapshot of metadata immediately upon any changes detected in the metadata (which itself periodically updates in scheduled intervals).

To be more specific about *what* metadata we handle, each component related to brokers and clusters have their own dedicated, immutable metadata records (ex: `ControllerMetadata`, `BrokerMetadata`, `PartitionMetadata`, etc) and are all contained within a `ClusterSnapshot` record which is what the Metadata instance periodically fetches from a cluster's `Controller` (pull-based). We set our default fetch interval to be 5 minutes however you may see in our codebase a much shorter interval for testing puroses. In order for the Controller to have the latest metadata for all of it's brokers, each broker in a cluster will periodically send its most up-to-date metadata to the controller via RPCs.

Some important usecases of metadata include:
- Producers must know how many partitions a broker has for the sake of partition selection for a particular message (more detail later)
- Controllers must know the details of their brokers in order to do partition assignment via topic creation
- Topic metadata is necessary so we know its # of partitions and per-partition metadata
- etc..

See the code [here](https://github.com/tferdous17/flux/tree/main/src/main/java/metadata) and check out the below diagram for a visual overview.
<img width="2954" height="984" alt="image" src="https://github.com/user-attachments/assets/15283044-2015-46a1-a039-beabecd6d774" />


# Internal Documentation

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
<dependency> <!-- necessary for Java 9+ -->
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

---

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
   protoc --java_out=target/generated-sources/protobuf/java --proto_path=src/main/proto src/main/proto/*.proto
   protoc --grpc-java_out=target/generated-sources/protobuf/grpc-java --proto_path=src/main/proto src/main/proto/*.proto
   ```
2. Re-run Maven:
   ```sh
   mvn compile
   ```
3. If you still get errors:
   - Check that the generated `.java` files exist in `target/generated-sources/protobuf/java/proto/` and `target/generated-sources/protobuf/grpc-java/proto/`.
   - Make sure your IDE recognizes these as source folders (sometimes you need to mark them as such).

  ```

