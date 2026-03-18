We can run in two `execution mode`: `STREAMING` (default) and `BATCH`

## Run with `STREAMING` mode
This mode can be used for both `bounded` or `unbounded` input source. This code uses `bounded` input source (for testing & demo purpose).
Then in production, actual input source can be `unbounded` stream.
```
flink run -Dexecution.runtime-mode=STREAMING examples/streaming/reduce-0.1-SNAPSHOT-all.jar
or
flink run examples/streaming/reduce-0.1-SNAPSHOT-all.jar
```

In this mode, continuous intermediate results are printed. Output:
```
ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
ElectronicOrder{electronic_id=HDTV, user_id=common_user_id, order_id=common_order_id, price=3999.23}
ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}
ElectronicOrder{electronic_id=ABCD, user_id=common_user_id, order_id=common_order_id, price=5833.98}
ElectronicOrder{electronic_id=HDTV, user_id=common_user_id, order_id=common_order_id, price=9000.21}
```

## Run with `BATCH` mode
Use this mode in `bounded` input source only.
```
flink run -Dexecution.runtime-mode=BATCH examples/streaming/reduce-0.1-SNAPSHOT-all.jar
```

With this mode, only final output is printed. Output:
```
ElectronicOrder{electronic_id=ABCD, user_id=common_user_id, order_id=common_order_id, price=5833.98}
ElectronicOrder{electronic_id=HDTV, user_id=common_user_id, order_id=common_order_id, price=9000.21}
```