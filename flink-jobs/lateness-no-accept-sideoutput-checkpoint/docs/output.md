# Checkpoint Recovery Demo

This use case keeps the event-time behavior from
`LatenessNoAcceptSideOutputDemo` and adds a deterministic task failure.
Checkpointing does not change whether an event is late. It only changes the
state and input position from which Flink recovers.

## Source layout

- `LatenessNoAcceptSideOutputCheckpointDemo` is the application entry point.
- `CheckpointDemoPipeline` assembles and identifies the stream operators.
- `DemoOptions` and `DemoEnvironmentFactory` handle arguments, restart
  strategy, and checkpoint configuration.
- `DemoOrderFactory`, `EagerWatermarkStrategyFactory`, and
  `EagerWatermarkGenerator` provide deterministic input and event-time logic.
- `RecoveryProbe` logs checkpoint lifecycle callbacks and injects the failure.
- `SumAllWindowFunction` logs and totals each event-time window.

## Baseline

With no arguments, the job behaves like the original lateness example:

```bash
./gradlew :lateness-no-accept-sideoutput-checkpoint:run
```

The first window produces `3999.23`, order `333` is sent to the late-record
side output, and the second window produces `6334.959999999999`.

## Restart without a checkpoint

```bash
./gradlew :lateness-no-accept-sideoutput-checkpoint:run \
  --args='--failure-demo true --checkpointing false'
```

The job processes through order `555`, then fails on a synthetic control
record. A fixed-delay restart is configured, but there is no checkpoint from
which to restore:

```text
RECOVERY PROBE: attempt=0, restored=false, restoredCheckpointId=none
...
output >>>> 3999.23
INJECTED FAILURE: failing attempt 0 after order 555
RECOVERY PROBE: attempt=1, restored=false, restoredCheckpointId=none
INPUT: attempt=1, order=111
...
output >>>> 3999.23
```

Attempt 1 starts from order `111`. The first-window result is printed twice
because the print sink already observed the attempt-0 result.

## Restart from a checkpoint

```bash
./gradlew :lateness-no-accept-sideoutput-checkpoint:run \
  --args='--failure-demo true --checkpointing true'
```

The paced input gives Flink time to snapshot the source, operator state, and
window state:

```text
CHECKPOINT COMPLETED: attempt=0, checkpoint=3
CHECKPOINT SNAPSHOT: attempt=0, checkpoint=4
INJECTED FAILURE: failing attempt 0 after order 555
RECOVERY PROBE: attempt=1, restored=true, restoredCheckpointId=4
INPUT: attempt=1, order=FAILURE_MARKER
INPUT: attempt=1, order=333
late-records >>>> ElectronicOrder{..., order_id=333, ...}
```

Attempt 1 resumes after order `555`; orders `111` through `555` are not
replayed, and `3999.23` is printed only once. The failure marker is filtered
out of the business data but advances the recovered stream watermark back to
`00:00:18`, ensuring order `333` remains too late.

The completion callback for the newest checkpoint can race with the injected
failure in the console output. `restoredCheckpointId=4` confirms that Flink
committed and selected checkpoint 4 for recovery.

## What this demonstrates

- A restart strategy decides whether Flink retries a failed task.
- A checkpoint supplies the consistent source position and operator/window
  state used by that retry.
- The local demo uses JobManager checkpoint storage. It survives this task
  restart, not a loss of the entire local Flink process.
- `print()` is not a transactional sink. Production end-to-end exactly-once
  output also requires a sink that participates in checkpoint commits.
