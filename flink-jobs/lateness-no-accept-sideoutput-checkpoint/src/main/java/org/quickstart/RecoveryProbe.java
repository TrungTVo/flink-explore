package org.quickstart;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.common.models.ElectronicOrder;

/**
 * Paces input, exposes checkpoint lifecycle callbacks, and injects one failure.
 */
final class RecoveryProbe
        extends RichMapFunction<ElectronicOrder, ElectronicOrder>
        implements CheckpointedFunction, CheckpointListener {

    private static final long DEMO_RECORD_DELAY_MS = 1_200L;
    private static final long NO_RESTORED_CHECKPOINT = -1L;

    private final boolean failureDemo;
    private int attemptNumber;
    private boolean restored;
    private long restoredCheckpointId = NO_RESTORED_CHECKPOINT;

    RecoveryProbe(boolean failureDemo) {
        this.failureDemo = failureDemo;
    }

    @Override
    public void open(OpenContext openContext) {
        attemptNumber = getRuntimeContext().getTaskInfo().getAttemptNumber();
        System.out.printf(
                "RECOVERY PROBE: attempt=%d, restored=%s, restoredCheckpointId=%s%n",
                attemptNumber,
                restored,
                restored ? Long.toString(restoredCheckpointId) : "none");
    }

    @Override
    public ElectronicOrder map(ElectronicOrder order) throws Exception {
        if (failureDemo) {
            Thread.sleep(DEMO_RECORD_DELAY_MS);
        }

        System.out.printf(
                "INPUT: attempt=%d, order=%s%n",
                attemptNumber, order.order_id);

        if (shouldInjectFailure(order)) {
            System.out.println("INJECTED FAILURE: failing attempt 0 after order 555");
            throw new RuntimeException("Expected checkpoint recovery demo failure");
        }

        return order;
    }

    private boolean shouldInjectFailure(ElectronicOrder order) {
        return failureDemo                                      // --failure-demo true
                && attemptNumber == 0                           // initial execution
                && DemoOrderFactory.isFailureMarker(order);     // order "FAILURE_MARKER" is the failure marker
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        restored = context.isRestored();
        restoredCheckpointId = context
                .getRestoredCheckpointId()
                .orElse(NO_RESTORED_CHECKPOINT);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        System.out.printf(
                "CHECKPOINT SNAPSHOT: attempt=%d, checkpoint=%d%n",
                attemptNumber, context.getCheckpointId());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        System.out.printf(
                "CHECKPOINT COMPLETED: attempt=%d, checkpoint=%d%n",
                attemptNumber, checkpointId);
    }
}
