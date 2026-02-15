# Run Flink Job

After building and packaging Flink Job into a fat `JAR`, push it into `Jobmanager` of Flink Cluster. Here, we use `Session Mode cluster`.
```
docker cp ./build/libs/quickstart-0.1-SNAPSHOT-all.jar jobmanager:/opt/flink/examples/streaming
```

Then we can run the Flink program by submitting Flink Job
```
flink run /opt/flink/examples/streaming/quickstart-0.1-SNAPSHOT-all.jar
```
