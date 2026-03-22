Build, package, and deploy the Flink job named "$ARGUMENTS" to the running Flink cluster.

Run the following commands in order using the Bash tool:

1. Clean the job:
```
./gradlew :$ARGUMENTS:clean
```

2. Compile the job:
```
./gradlew :$ARGUMENTS:compileJava
```

3. Package the fat JAR:
```
./gradlew :$ARGUMENTS:shadowJar
```

4. Copy the JAR into the Flink jobmanager container:
```
docker cp ./flink-jobs/$ARGUMENTS/build/libs/$ARGUMENTS-0.1-SNAPSHOT-all.jar jobmanager:/opt/flink/examples/streaming/
```

If any step fails, stop and report the error. On success, confirm the JAR was deployed to the cluster.
