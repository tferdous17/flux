# FLUX

### A work-in-progress distributed event streaming and message queue platform engineered for high throughput, maximal scalability, and fault-tolerance.
(aka an Apache Kafka clone)

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

