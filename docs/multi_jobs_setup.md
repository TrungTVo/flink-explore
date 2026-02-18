# Set Up Structure for Multiple Flink Jobs development

Sample Structure
```
flink-explore/
|-- settings.gradle
|-- gradle.properties
|-- build.gradle                              # (optional, for shared config)
|-- flink-jobs/
    |-- common/                               # (optional, for shared code)
    |   |-- build/
    |   |   |-- classes/
    |   |   |-- libs/
    |   |       |-- common-0.1.jar           # thin JAR
    |   |-- build.gradle
    |   |-- src/
    |       |-- main/
    |           |-- java/
    |               |-- org/
    |                   |-- quickstart/
    |                       |-- JobBaseCommon.java
    |-- job1/
    |   |-- build/
    |   |   |-- classes/
    |   |   |-- libs/
    |   |       |-- job1-0.1-all.jar         # shadow/fat JAR
    |   |-- build.gradle
    |   |-- src/
    |       |-- main/
    |           |-- java/
    |               |-- org/
    |                   |-- quickstart/
    |                       |-- Job1Main.java
    `-- job2/
        |-- build/
        |   |-- classes/
        |   |-- libs/
        |       |-- job2-0.1-all.jar         # shadow/fat JAR
        |-- build.gradle
        |-- src/
            |-- main/
                |-- java/
                    |-- org/
                        |-- quickstart/
                            |-- Job2Main.java
```

## How to run individual Flink Job

### For Common Module
```
./gradlew :common:clean
./gradlew :common:compileJava
./gradlew :common:jar
```

### For Individual Flink Job
```
./gradlew :job1:clean
./gradlew :job1:compileJava
./gradlew :job1:shadowJar
```

Inspecting dependencies of Flink Job's fat JAR
```
./gradlew :job1:dependencies --configuration flinkShadowJar
```

If your IDE cannot recognize all dependencies, including custom configurations like `flinkShadowJar`. Run:
```
./gradlew :job1:cleanEclipse
./gradlew :job1:eclipse
```

Check Flink Job classpath from its fat JAR
```
jar tf flink-jobs/job1/build/libs/job1-0.1-all.jar
```

Push Flink Job's fat JAR and common thin JAR to Flink Cluster
```
docker cp ./flink-jobs/job1/build/libs/job1-0.1-all.jar jobmanager:/opt/flink/examples/streaming/
docker cp ./flink-jobs/common/build/libs/common-0.1.jar jobmanager:/opt/flink/lib
```

Execute Flink Job
```
flink run /opt/flink/examples/streaming/job1-0.1-all.jar
```
