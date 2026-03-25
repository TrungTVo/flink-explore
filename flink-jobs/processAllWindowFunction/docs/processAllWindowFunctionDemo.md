## Output

This demo uses `event time` window with hardcoded timestamp for each bounded event record.

Sample output:

```bash
[2026-01-01T00:00 - 2026-01-01T00:00:10], process records batch of: 3, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=aiko, order_id=222, price=1999.23}
	Processing input record: ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}

[2026-01-01T00:00:10 - 2026-01-01T00:00:20], process records batch of: 2, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=ABCD, user_id=aiko, order_id=444, price=1333.98}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=555, price=5000.98}

output:2> 8499.23
output:3> 6334.959999999999
```

## Note

Processing time windows don't work well with bounded/batch sources because the data is consumed faster than the window duration. Event time windows are the correct approach for bounded datasets with embedded timestamps.

For demo that uses `processing time` window, it's better to have unbounded source. Refer to `word-count` example.

Rules of thumb
```
bounded source -> event time
unbounded source -> processing/event time
```
