## When to use this pattern with `ProcessFunction`?

Use `ProcessFunction` with side outputs when:
* classification logic is more than simple filter
* you want to inspect each record once
* you need multiple downstream streams
* you want to preserve rejected records instead of dropping them
* you need clean stream branching
* handle rejected records in Dead Letter Queue of Kafka (production use case)
* attach the event timestamp to the output
* inspect processing context

### Another example: **e-commerce orders**
* valid paid order → main output
* missing customer ID → side output
* suspicious high-value order → another side output

One input stream can be split into multiple streams by category.

## Real examples where `.process()` is the right tool

### Inactivity detection
```
If no event from device for 60 seconds, emit offline alert
```

### Fraud rule
```
If same card has 3 failed payments in 2 minutes, emit fraud signal
```

### Deduplication
```
If eventId was seen before, drop it
```

### Timeout workflow
```
Order created but payment not received within 15 minutes -> emit timeout
```

### Stream join with custom timeout
```
Match order event with shipment event; if shipment never arrives, emit missing-shipment warning
```

### Side outputs
```
Valid events to main stream, malformed JSON to dead-letter stream
```