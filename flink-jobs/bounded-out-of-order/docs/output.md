# BoundedOutOfOrderDemo output

This demo reads records from a socket and assigns event time from the last CSV
field.

```text
orderId,userId,electronicId,price,eventOffsetSeconds
```

Start the socket inside the `taskmanager` container:

```bash
nc -lk 9999
```

Then send these records slowly, with at least a short pause between lines. The
demo sets `env.getConfig().setAutoWatermarkInterval(500L)`, so Flink checks for
new periodic watermarks every 500 ms.

```text
111,trung,HDTV,2000.00,0
222,aiko,HDTV,1999.23,4
444,aiko,ABCD,1333.98,12
555,trung,HDTV,5000.98,17
333,trung,ABCD,4500.00,8
999,system,TICK,0.00,25
```

The `999` record is a zero-price tick. Because `socketTextStream` is unbounded,
there is no final end-of-input watermark, so a later event is needed to advance
the watermark and close later windows.

## Window setup

```java
WatermarkStrategy
    .<ElectronicOrder>forBoundedOutOfOrderness(Duration.ofSeconds(B))
    .withTimestampAssigner((inputRecord, ts) -> inputRecord.timestamp)

TumblingEventTimeWindows.of(Duration.ofSeconds(10))
allowedLateness(Duration.ofSeconds(5))
```

Windows:

```text
W1 = [00:00:00, 00:00:10)
W2 = [00:00:10, 00:00:20)
```

For `forBoundedOutOfOrderness(B)`, Flink periodically emits:

```text
watermark = maxSeenEventTime - B - 1 ms
```

The `-1 ms` matters because Flink windows use an inclusive max timestamp. For a
10-second window, `W1` fires when the watermark reaches `00:00:09.999`.

`W1` cleanup time:

```text
window max timestamp + allowed lateness
= 00:00:09.999 + 5 seconds
= 00:00:14.999
```

If record `333 @ 00:00:08` arrives after the current watermark has reached
`00:00:14.999`, then `W1` has already been cleaned up and record `333` is
dropped.

## Event prices

```text
111 = 2000.00  @ 00:00:00
222 = 1999.23  @ 00:00:04
444 = 1333.98  @ 00:00:12
555 = 5000.98  @ 00:00:17
333 = 4500.00  @ 00:00:08
999 = 0.00     @ 00:00:25
```

Window totals:

```text
111 + 222       = 3999.23
111 + 222 + 333 = 8499.23
444 + 555       = 6334.96
```

## Case 1: `forBoundedOutOfOrderness(0 seconds)`

Watermark calculation:

```text
After 111 @ 00:00:00 -> watermark = 00:00:00.000 - 0s - 1ms = 23:59:59.999
After 222 @ 00:00:04 -> watermark = 00:00:04.000 - 0s - 1ms = 00:00:03.999
After 444 @ 00:00:12 -> watermark = 00:00:12.000 - 0s - 1ms = 00:00:11.999
After 555 @ 00:00:17 -> watermark = 00:00:17.000 - 0s - 1ms = 00:00:16.999
After 333 @ 00:00:08 -> max seen is still 00:00:17, watermark stays 00:00:16.999
After 999 @ 00:00:25 -> watermark = 00:00:25.000 - 0s - 1ms = 00:00:24.999
```

Intermediate results:

```text
After 444:
  watermark = 00:00:11.999
  W1 fires because watermark >= 00:00:09.999
  W1 contains 111, 222
  output: 3999.23

After 555:
  watermark = 00:00:16.999
  W1 cleanup has passed because watermark >= 00:00:14.999

After 333:
  333 belongs to W1, but W1 has already been cleaned up
  333 is dropped

After 999:
  watermark = 00:00:24.999
  W2 fires with 444, 555
  output: 6334.96
```

Expected output:

```text
output> 3999.23
output> 6334.96
```

Record `333` is dropped.

## Case 2: `forBoundedOutOfOrderness(2 seconds)`

Watermark calculation:

```text
After 444 @ 00:00:12 -> watermark = 00:00:12.000 - 2s - 1ms = 00:00:09.999
After 555 @ 00:00:17 -> watermark = 00:00:17.000 - 2s - 1ms = 00:00:14.999
After 333 @ 00:00:08 -> max seen is still 00:00:17, watermark stays 00:00:14.999
After 999 @ 00:00:25 -> watermark = 00:00:25.000 - 2s - 1ms = 00:00:22.999
```

Intermediate results:

```text
After 444:
  watermark = 00:00:09.999
  W1 fires with 111, 222
  output: 3999.23

After 555:
  watermark = 00:00:14.999
  W1 cleanup time is reached

After 333:
  333 belongs to W1, but W1 has already been cleaned up
  333 is dropped

After 999:
  W2 fires with 444, 555
  output: 6334.96
```

Expected output:

```text
output> 3999.23
output> 6334.96
```

Record `333` is dropped.

## Case 3: `forBoundedOutOfOrderness(3 seconds)`

This is the value currently used in `BoundedOutOfOrderDemo.java`.

Watermark calculation:

```text
After 444 @ 00:00:12 -> watermark = 00:00:12.000 - 3s - 1ms = 00:00:08.999
After 555 @ 00:00:17 -> watermark = 00:00:17.000 - 3s - 1ms = 00:00:13.999
After 333 @ 00:00:08 -> max seen is still 00:00:17, watermark stays 00:00:13.999
After 999 @ 00:00:25 -> watermark = 00:00:25.000 - 3s - 1ms = 00:00:21.999
```

Intermediate results:

```text
After 444:
  watermark = 00:00:08.999
  W1 does not fire yet

After 555:
  watermark = 00:00:13.999
  W1 fires because watermark >= 00:00:09.999
  W1 cleanup has not happened because watermark < 00:00:14.999
  W1 contains 111, 222
  output: 3999.23

After 333:
  333 belongs to W1
  W1 has fired, but it is still within allowed lateness
  W1 fires again with 111, 222, 333
  output: 8499.23

After 999:
  watermark = 00:00:21.999
  W2 fires with 444, 555
  output: 6334.96
```

Expected output:

```text
output> 3999.23
output> 8499.23
output> 6334.96
```

Record `333` is not dropped.

## Case 4: `forBoundedOutOfOrderness(5 seconds)`

Watermark calculation:

```text
After 444 @ 00:00:12 -> watermark = 00:00:12.000 - 5s - 1ms = 00:00:06.999
After 555 @ 00:00:17 -> watermark = 00:00:17.000 - 5s - 1ms = 00:00:11.999
After 333 @ 00:00:08 -> max seen is still 00:00:17, watermark stays 00:00:11.999
After 999 @ 00:00:25 -> watermark = 00:00:25.000 - 5s - 1ms = 00:00:19.999
```

Intermediate results:

```text
After 444:
  watermark = 00:00:06.999
  W1 does not fire yet

After 555:
  watermark = 00:00:11.999
  W1 fires with 111, 222
  W1 cleanup has not happened
  output: 3999.23

After 333:
  333 belongs to W1
  W1 fires again with 111, 222, 333
  output: 8499.23

After 999:
  watermark = 00:00:19.999
  W2 fires with 444, 555
  output: 6334.96
```

Expected output:

```text
output> 3999.23
output> 8499.23
output> 6334.96
```

Record `333` is not dropped.

## Summary

With this input order, record `333` is dropped only when the out-of-orderness
bound is small enough that `555 @ 00:00:17` advances the watermark to the first
window cleanup time before `333` arrives.

```text
333 is dropped when:

watermark after 555 >= W1 cleanup time
17s - B - 1ms >= 14.999s
B <= 2s
```

Result table:

| `forBoundedOutOfOrderness` | Watermark after `555` | Is `333` dropped? | Output |
| --- | ---: | --- | --- |
| `0 seconds` | `00:00:16.999` | Yes | `3999.23`, `6334.96` |
| `1 second` | `00:00:15.999` | Yes | `3999.23`, `6334.96` |
| `2 seconds` | `00:00:14.999` | Yes | `3999.23`, `6334.96` |
| `3 seconds` | `00:00:13.999` | No | `3999.23`, `8499.23`, `6334.96` |
| `5 seconds` | `00:00:11.999` | No | `3999.23`, `8499.23`, `6334.96` |

If you use a larger bound, later watermarks move more slowly. For example, with
`8 seconds`, the tick record at `25s` only creates watermark `16.999s`, so `W2`
does not fire yet. You would need a later tick such as `28s` or greater to close
`W2`.

References:

- Apache Flink `WatermarkStrategy.forBoundedOutOfOrderness(...)` JavaDoc:
  <https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/common/eventtime/WatermarkStrategy.html>
- Apache Flink generating watermarks docs:
  <https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/>
