## Start Cluster in Application Mode
```
docker compose -f ./application-mode/docker-compose.yml up -d
```

In this mode, a `jobmanager` runs a single Flink Job. In other words, each job will create a new `jobmanager`. It's IMPORTANT to know that when your application (job) completes, the entire cluster shuts down automatically. This is one of the key differences between Session Mode and Application Mode.

Because in Application Mode:

* The cluster is created for a single application.
* That application is run as the cluster’s entrypoint.
* When the application ends, the cluster has no more work to do, so it shuts itself down.
* Resources (K8s pods, YARN containers, or Docker containers) are released.

Sample Job (default) is specified in `application-mode/docker-compose.yml` as:
```
command: standalone-job --job-classname org.apache.flink.streaming.examples.windowing.TopSpeedWindowing --jars ../opt/flink/artifacts/WordCount.jar, ../opt/flink/artifacts/TopSpeedWindowing.jar
```

List of `--jars` files can be separated by comma, but only a single job with its classname is executed by specified `--job-classname`

## Start Cluster in Session Mode
```
docker compose -f ./session-mode/docker-compose.yml up -d
```

### Submit Flink Job
Go into `jobmanager` container and submit a sample Flink Job with sample `.jar` files in `examples` folder (Or we can create a simple flink app and submit)
```
./bin/flink run examples/streaming/TopSpeedWindowing.jar
```

## Access SQL Client container
Access into SQL Client Server Container:
```
docker exec -it sql-client /bin/bash
```

Start SQL Client shell
```
./bin/sql-client.sh
```

We should see:
```
Flink SQL> start querying...
```