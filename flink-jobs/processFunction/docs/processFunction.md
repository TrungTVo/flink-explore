## Conceptually
```
input records
    ↓
ProcessFunction
    ├── main output   -> normal records
    └── side output   -> special records
```

## Why do we need side output?

Because sometimes not all records should go through the same path. For example:
* good records go to normal pipeline
* bad records go to error pipeline
* suspicious records go to fraud pipeline
* late records go to late-event pipeline

Instead of mixing everything together, Flink lets you split them.

## Run with `STREAMING` mode
```
process: xyz, subtask=2/4
process: abc, subtask=0/4
process: 200, subtask=1/4
INVALID:1> abc
INVALID:3> xyz
process: 100, subtask=3/4
VALID:4> 100
process: 300, subtask=3/4
VALID:4> 300
VALID:2> 200
```

## Run with `BATCH` mode
```
process: abc, subtask=0/1
INVALID> abc
process: 200, subtask=0/1
VALID> 200
process: xyz, subtask=0/1
INVALID> xyz
process: 100, subtask=0/1
VALID> 100
process: 300, subtask=0/1
VALID> 300
```