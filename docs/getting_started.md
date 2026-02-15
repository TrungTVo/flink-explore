# Getting Started

This doc explains how to get started writing Flink Application. Some prerequisite steps need to be configured for Flink Dependencies management.

## Managing Flink Dependencies in build.gradle

This project uses a best-practice approach to manage Flink dependencies and control what gets packaged into your job JAR (a.k.a fat/uber/shadow JAR). Here’s how it works and why:

### 1. Separate Core and Connector Dependencies

- **Flink core libraries** (e.g., `flink-streaming-java`, `flink-clients`) are marked as `compileOnly`:
  ```gradle
  compileOnly "org.apache.flink:flink-streaming-java:${flinkVersion}"
  compileOnly "org.apache.flink:flink-clients:${flinkVersion}"
  ```
  > These are needed for compilation and IDE support, but **should NOT be bundled** in your job JAR. The Flink cluster provides them at runtime.

- **Flink connectors** (e.g., `flink-connector-files`) are added to a custom configuration (`flinkShadowJar`):
  ```gradle
  configurations {
      flinkShadowJar
      // ...exclusions...
  }
  flinkShadowJar "org.apache.flink:flink-connector-files:${flinkVersion}"
  ```
  > These are the only Flink dependencies that get bundled into your fat JAR, so your job can use connectors not present on the cluster.

### 2. Why Not Use `implementation` for Connectors?

- Using `implementation` would add connectors (and their transitive dependencies) to both compile and runtime classpaths, and by default, to the shadow JAR. This can accidentally include Flink core/runtime classes, causing version conflicts or classpath issues on the cluster.
- The custom `flinkShadowJar` configuration gives you **precise control** over what is included in the fat JAR, avoiding accidental bundling of core Flink libraries.

### 3. Making Connectors Available for Compilation

- The following block ensures that your custom connector dependencies are available for compilation and running locally, even though they are not in `implementation`:
  ```gradle
  sourceSets {
      main.compileClasspath += configurations.flinkShadowJar
      main.runtimeClasspath += configurations.flinkShadowJar
      // ...
  }
  ```

### 4. Excluding Unwanted Dependencies

- The `flinkShadowJar` configuration is set up to exclude logging and core Flink modules that should not be bundled:
  ```gradle
  flinkShadowJar.exclude group: 'org.apache.flink', module: 'force-shading'
  flinkShadowJar.exclude group: 'org.slf4j'
  flinkShadowJar.exclude group: 'org.apache.logging.log4j'
  // ...
  ```

### 5. Building and Inspecting the Fat JAR

- To check custom Flink dependencies in your app (e.g. Connectors):
  ```sh
  ./gradlew dependencies --configuration flinkShadowJar
  ```

- To build your job JAR:
  ```sh
  ./gradlew clean compileJava --info
  ./gradlew shadowJar
  ```
- To inspect its contents:
  ```sh
  jar tf build/libs/quickstart-0.1-SNAPSHOT-all.jar
  ```
  > Only connector and your own classes should be present, not Flink core dependencies.

### 6. Summary Table

| Dependency Type                   | Gradle Config   | Included in Fat JAR? | Provided by Flink Cluster? |
|-----------------------------------|-----------------|----------------------|-----------------------------|
| Flink core/runtime                | compileOnly     | ❌                   | ✅                          |
| Flink custom deps (connectors)    | flinkShadowJar  | ✅                   | ❌                          |
| Logging/test utils                | runtimeOnly     | (optional)           | (local only)                |

### Why run `./gradlew cleanEclipse eclipse`?

If you use VSCode (or Eclipse) for Java development, running this command regenerates the `.classpath` and `.project` files. These files help the Java language server (used by VSCode) recognize all dependencies, including custom configurations like `flinkShadowJar`. This ensures that code completion, import resolution, and error checking work correctly in your IDE, especially after you add or change dependencies in `build.gradle`.

> **Tip:** If you see unresolved import errors in VSCode after changing dependencies, run this command and reload your project.

---

This setup ensures your job JAR is portable, minimal, and compatible with the Flink cluster. For more details, see the comments in `build.gradle`.