# When to use `event time` vs `processing time`?

* bounded source → usually event time
* unbounded source → often event time, sometimes processing time

## Why?

### **Bounded source**

This is usually historical data, files, replayed records, or a finite dataset.

You normally care about:

* when the events actually happened
* grouping old data by its embedded timestamps

So `event time` is usually the right fit.

Processing time on bounded data often just reflects how fast you replayed the file, which is usually not meaningful.

### **Unbounded source**

This is usually live Kafka, sockets, sensors, user activity, ongoing logs.

Here you can use either:

* `event time` if you care about when the event really happened
* `processing time` if you care about when Flink received/processed it

**So unbounded does not automatically mean processing time.**

In short, `processing time` is essentially tied to the wall clock of the machine/operator doing the work. Flink’s own docs describe processing-time windows as being defined “with respect to the wall clock of the machine that builds and processes a window.”

## Better mental model

Choose based on what question you are asking, not just source type.

Use event time when asking:

* “When did this really happen?”
* “Put late events in the correct window”
* “Compute metrics by actual business time”
* use cases
    * clicks
    * orders
    * payments
    * sensor readings
    * mobile/offline events
    * logs that may arrive late

Use processing time when asking:

* “What is arriving right now?”
* “Flush every 10 seconds of wall clock”
* “Fire a timeout 30 seconds after I see a record”
* use cases
    * Real-time dashboard of incoming traffic (“Show how many records are arriving per second into Flink right now”)
    * Timeout / inactivity detection in the job itself (“If I haven’t seen any record for this key in 30 seconds, mark it idle”)

## Simple cheat sheet

* bounded + historical timestamps → event time
* unbounded + analytics/business events → event time
* unbounded + operational/live behavior → processing time

So your summary is close, but I’d refine it to:

* Bounded source → usually event time
* Unbounded source → event time or processing time depending on what you care about

The biggest correction is: unbounded does not imply processing time. In real Flink apps, many unbounded streams still use event time.