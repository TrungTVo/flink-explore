## Output

`LatenessAcceptDemo` uses event time with a 10-second tumbling window and
5 seconds of allowed lateness:

```java
.windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
.allowedLateness(Duration.ofSeconds(5))
```

The custom watermark generator emits a watermark on every record at the highest
event timestamp seen so far. Because the input is bounded, Flink also advances
the watermark to the end when the source finishes, which flushes any remaining
open windows.

## Input Event Order

The records are processed in this order:

| Arrival | Order ID | Event Time | Window |
| --- | --- | --- | --- |
| 1 | `111` | `00:00:00` | `[00:00:00, 00:00:10)` |
| 2 | `222` | `00:00:04` | `[00:00:00, 00:00:10)` |
| 3 | `444` | `00:00:12` | `[00:00:10, 00:00:20)` |
| 4 | `555` | `00:00:14` | `[00:00:10, 00:00:20)` |
| 5 | `333` | `00:00:08` | `[00:00:00, 00:00:10)` |

Record `333` arrives out of order because its event time belongs to the first
window, but it is processed after records from the second window.

## Watermark Timeline

| After Record | Watermark | Effect |
| --- | --- | --- |
| `111` | `00:00:00` | First window is still collecting records. |
| `222` | `00:00:04` | First window is still collecting records. |
| `444` | `00:00:12` | First window fires because the watermark has passed `00:00:10`. |
| `555` | `00:00:14` | First window state is still retained because allowed lateness keeps it until `00:00:15`. |
| `333` | `00:00:14` | Late record is accepted and the first window fires again with the updated contents. |
| End of source | final watermark | Second window fires. |

With the current code, `333` is accepted because the watermark is only
`00:00:14` when it arrives. The first window is retained until its end time plus
allowed lateness: `00:00:10 + 5 seconds = 00:00:15`.

If a later record advanced the watermark to `00:00:15` or later before `333`
arrived, then `333` would be dropped because no late-data side output is
configured.

## Expected Console Output

Assuming the JVM default timezone prints the window boundaries as UTC, the
expected output is:

```text
[2026-01-01T00:00 - 2026-01-01T00:00:10], process records batch of: 2, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=aiko, order_id=222, price=1999.23}

output> 3999.23

[2026-01-01T00:00 - 2026-01-01T00:00:10], process records batch of: 3, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=aiko, order_id=222, price=1999.23}
	Processing input record: ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}

output> 8499.23

[2026-01-01T00:00:10 - 2026-01-01T00:00:20], process records batch of: 2, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=ABCD, user_id=aiko, order_id=444, price=1333.98}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=555, price=5000.98}

output> 6334.959999999999
```

The first window appears twice:

1. Initial firing after record `444` advances the watermark past `00:00:10`.
2. Late firing after record `333` arrives while the window is still within the
   5-second allowed-lateness period.
