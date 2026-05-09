## Output

This job demonstrates that a record can be dropped as late when the watermark has
already advanced beyond the record's window plus the configured allowed lateness.

The input records are emitted in this processing order:

| Order ID | Event time | Window |
| --- | --- | --- |
| `111` | `00:00:00` | `[00:00:00, 00:00:10)` |
| `222` | `00:00:04` | `[00:00:00, 00:00:10)` |
| `444` | `00:00:12` | `[00:00:10, 00:00:20)` |
| `555` | `00:00:18` | `[00:00:10, 00:00:20)` |
| `333` | `00:00:08` | `[00:00:00, 00:00:10)` |

Record `333` belongs to the first window, but it arrives after records from the
second window.

## Watermark logic

The job assigns event time from each record:

```java
.withTimestampAssigner((inputRecord, ts) -> inputRecord.timestamp)
```

It also uses a custom `WatermarkGenerator`:

```java
private long maxTimestamp = Long.MIN_VALUE;

@Override
public void onEvent(ElectronicOrder event, long eventTimestamp, WatermarkOutput output) {
    maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    output.emitWatermark(new Watermark(maxTimestamp));
}
```

For every input record, the generator remembers the largest event timestamp seen
so far and immediately emits that value as the watermark.

That makes watermark progress deterministic for the demo:

| After record | Event time | Watermark |
| --- | --- | --- |
| `111` | `00:00:00` | `00:00:00` |
| `222` | `00:00:04` | `00:00:04` |
| `444` | `00:00:12` | `00:00:12` |
| `555` | `00:00:18` | `00:00:18` |
| `333` | `00:00:08` | still `00:00:18` |

The first tumbling window is:

```text
[00:00:00, 00:00:10)
```

The job allows 5 seconds of lateness:

```java
.allowedLateness(Duration.ofSeconds(5))
```

So the first window can still accept late records until roughly:

```text
window end + allowed lateness = 00:00:10 + 5s = 00:00:15
```

When record `333` arrives, the current watermark is already `00:00:18`, which is
past the first window's allowed-lateness period. Because the job does not define
a side output for late data, record `333` is silently dropped.

## Expected output

```
[2026-01-01T00:00 - 2026-01-01T00:00:10], process records batch of: 2, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=aiko, order_id=222, price=1999.23}

output> 3999.23

[2026-01-01T00:00:10 - 2026-01-01T00:00:20], process records batch of: 2, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=ABCD, user_id=aiko, order_id=444, price=1333.98}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=555, price=5000.98}

output> 6334.959999999999
```

The first output is:

```text
2000.00 + 1999.23 = 3999.23
```

The second output is:

```text
1333.98 + 5000.98 = 6334.959999999999
```

Record `333` with price `4500.00` is not included in either sum.

Window timestamps are printed with `ZoneId.systemDefault()` in the Java code, so
the displayed wall-clock time may shift if the job runs on a machine with a
different local timezone.
