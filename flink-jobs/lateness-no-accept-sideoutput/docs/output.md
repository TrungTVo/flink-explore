## Output

```
[2026-01-01T00:00 - 2026-01-01T00:00:10], process records batch of: 2, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=aiko, order_id=222, price=1999.23}

output >>>> 3999.23

late-records >>>> ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}

[2026-01-01T00:00:10 - 2026-01-01T00:00:20], process records batch of: 2, subtask=0/1
	Processing input record: ElectronicOrder{electronic_id=ABCD, user_id=aiko, order_id=444, price=1333.98}
	Processing input record: ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=555, price=5000.98}

output >>>> 6334.959999999999
```