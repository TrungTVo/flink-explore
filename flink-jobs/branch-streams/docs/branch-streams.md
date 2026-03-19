## Conceptually
```
electronicOrders
      |
      +--> b1: keyBy(electronic_id)             --> reduce --> print
      |
      +--> b2: keyBy(common_key_partition)      --> reduce --> print
```

## Run with `STREAMING` mode
This mode can be used for both `bounded` or `unbounded` input source. This code uses `bounded` input source (for testing & demo purpose).
Then in production, actual input source can be `unbounded` stream.
```
flink run -Dexecution.runtime-mode=STREAMING examples/streaming/branch-streams-0.1-SNAPSHOT-all.jar
or
flink run examples/streaming/branch-streams-0.1-SNAPSHOT-all.jar
```

In this mode, continuous intermediate results are printed. Output:
```
# Events flow in, assigned to taskId based on parallelism
assignedKey=common_key_partition, currentSubtask=2, ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}
assignedKey=common_key_partition, currentSubtask=1, ElectronicOrder{electronic_id=HDTV, user_id=aiko, order_id=222, price=1999.23}
assignedKey=common_key_partition, currentSubtask=0, ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
assignedKey=common_key_partition, currentSubtask=3, ElectronicOrder{electronic_id=ABCD, user_id=aiko, order_id=444, price=1333.98}
assignedKey=common_key_partition, currentSubtask=0, ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=555, price=5000.98}

# branch 1, compute total price for each electronic_id
1> ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}
1> ElectronicOrder{electronic_id=ABCD, user_id=common_user_id_b1, order_id=common_order_id_b1, price=5833.98}
3> ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
3> ElectronicOrder{electronic_id=HDTV, user_id=common_user_id_b1, order_id=common_order_id_b1, price=3999.23}
3> ElectronicOrder{electronic_id=HDTV, user_id=common_user_id_b1, order_id=common_order_id_b1, price=9000.21}

# branch 2, compute total price for all (uses only one bucket: common_key_partition)
3> ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}
3> ElectronicOrder{electronic_id=common_key_partition, user_id=common_user_id_b2, order_id=common_order_id_b2, price=6499.23}
3> ElectronicOrder{electronic_id=common_key_partition, user_id=common_user_id_b2, order_id=common_order_id_b2, price=7833.21}
3> ElectronicOrder{electronic_id=common_key_partition, user_id=common_user_id_b2, order_id=common_order_id_b2, price=9833.21}
3> ElectronicOrder{electronic_id=common_key_partition, user_id=common_user_id_b2, order_id=common_order_id_b2, price=14834.19}
```

Note that these output logs can interleave each other.

## Run with `BATCH` mode
Use this mode in `bounded` input source only.
```
flink run -Dexecution.runtime-mode=BATCH examples/streaming/branch-streams-0.1-SNAPSHOT-all.jar
```

With this mode, only final output is printed. Output:
```
# Events flow in
assignedKey=common_key_partition, currentSubtask=0, ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=111, price=2000.00}
assignedKey=common_key_partition, currentSubtask=0, ElectronicOrder{electronic_id=HDTV, user_id=trung, order_id=555, price=5000.98}
assignedKey=common_key_partition, currentSubtask=0, ElectronicOrder{electronic_id=HDTV, user_id=aiko, order_id=222, price=1999.23}
assignedKey=common_key_partition, currentSubtask=0, ElectronicOrder{electronic_id=ABCD, user_id=trung, order_id=333, price=4500.00}
assignedKey=common_key_partition, currentSubtask=0, ElectronicOrder{electronic_id=ABCD, user_id=aiko, order_id=444, price=1333.98}

# branch 1, compute total price for each electronic_id
ElectronicOrder{electronic_id=ABCD, user_id=common_user_id_b1, order_id=common_order_id_b1, price=5833.98}
ElectronicOrder{electronic_id=HDTV, user_id=common_user_id_b1, order_id=common_order_id_b1, price=9000.21}

# branch 2, compute total price for all (uses only one bucket: common_key_partition)
ElectronicOrder{electronic_id=common_key_partition, user_id=common_user_id_b2, order_id=common_order_id_b2, price=14834.19}
```