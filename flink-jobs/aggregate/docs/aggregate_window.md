## Output

```
key=ABCD, [2026-01-01T00:00 - 2026-01-01T00:00:10], process records batch of: 1, average price: 16200.0
output:1> 16200.0
key=ABCD, [2026-01-01T00:00:10 - 2026-01-01T00:00:20], process records batch of: 1, average price: 1333.98
output:1> 1333.98
key=HDTV, [2026-01-01T00:00 - 2026-01-01T00:00:10], process records batch of: 1, average price: 4000.0
output:3> 4000.0
key=HDTV, [2026-01-01T00:00:10 - 2026-01-01T00:00:20], process records batch of: 1, average price: 5000.98
output:3> 5000.98
```

Note that batch size of each window is always 1 because we are using an incremental `AggregateFunction` that updates the average with each incoming record. Each input record is added to existing accumulator but abandoned after the window is evaluated. Thus the batch size is effectively 1 for each window evaluation, even though the window may have received multiple records. The average price is updated incrementally as each record arrives, and the final average is emitted when the window is triggered.