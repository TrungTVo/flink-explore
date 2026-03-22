# Flink Explore

A collection of Apache Flink streaming job demos built with Gradle and Java 21.

## Project Structure

- **Root build**: `build.gradle` — multi-project Gradle build with shared config
- **`flink-jobs/`** — each subdirectory is an independent Flink job subproject:
  - `common/` — shared models and base classes (not a runnable job)
  - `word-count/` — basic WordCount example
  - `stream-files-connector/` — file connector demo
  - `reduce/` — reduce operator demo
  - `branch-streams/` — branching/splitting streams
  - `processFunction/` — ProcessFunction API demo
  - `keyedProcessFunction/` — KeyedProcessFunction API demo
- **`deployment/`** — Docker Compose files for `session-mode` and `application-mode` clusters

## Tech Stack

- **Java 21**, **Flink 2.2.0**, **Gradle** (with configuration cache, parallel builds, and caching enabled)
- Shadow plugin (`com.gradleup.shadow` 9.3.1) for building fat JARs
- Logging: Log4j 2.24.3 + SLF4J 2.0.16

## Build & Run

```bash
# Build all jobs (fat JARs)
./gradlew shadowJar

# Run a specific job locally
./gradlew :word-count:run
./gradlew :reduce:run
./gradlew :branch-streams:run
./gradlew :processFunction:run
./gradlew :keyedProcessFunction:run

# Start Flink cluster (session mode)
docker compose -f deployment/session-mode/docker-compose.yml up -d

# Start Flink cluster (application mode)
docker compose -f deployment/application-mode/docker-compose.yml up -d
```

## Conventions

- Each new Flink job goes in its own directory under `flink-jobs/` and must be registered in `settings.gradle`.
- Job subprojects get `application`, `shadow`, and `eclipse` plugins automatically (configured in root `build.gradle`).
- Flink core dependencies (`flink-streaming-java`, `flink-clients`) are `compileOnly` — they are provided by the Flink cluster at runtime.
- Additional runtime dependencies (connectors, etc.) are for job-specific and go in the `flinkShadowJar` configuration so they are bundled in the fat JAR.
- The `common` module is excluded from job-specific plugin configuration.
