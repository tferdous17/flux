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

