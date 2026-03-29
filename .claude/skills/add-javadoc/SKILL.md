---
name: add-javadoc
description: Use this skill whenever the user asks to add comments, add Javadoc, document a Java class or method, add block comments, or generate documentation for Java source files. Trigger on phrases like "add comments", "add javadoc", "document this", "comment this class/method/interface", "add block comments" — even if they don't say "javadoc" explicitly. Use this skill proactively when the user opens or pastes Java code that is missing documentation.
---

# Add Javadoc Comments

Add Javadoc block comments (`/** ... */`) to Java source files. Target classes, interfaces, enums, constructors, and methods. Aim for **moderate detail** — concise enough to read quickly, thorough enough to understand without reading the implementation.

## Process

1. Read the file if not already in context
2. Identify every undocumented top-level and nested declaration (class, interface, enum, constructor, method)
3. Add comments top-down, from type declarations → constructors → methods
4. **Leave existing comments untouched** — don't overwrite or reformat existing Javadoc

## Comment Style

### Classes, Interfaces, Enums

One sentence describing the role of this type. If the purpose isn't obvious from the name alone, add a second sentence explaining what it does within the broader system.

```java
/**
 * Flink AggregateFunction that incrementally computes the average price
 * of {@link ElectronicOrder} records within a window.
 */
private static class AvgWindowFunction implements AggregateFunction<...> { ... }
```

### Methods and Constructors

Lead with what the method does (not how). Add `@param`, `@return`, and `@throws` only when they add clarity beyond the method signature.

```java
/**
 * Merges two partial accumulators by summing their totals and counts.
 *
 * @param acc1 left accumulator
 * @param acc2 right accumulator
 * @return combined accumulator
 */
public Tuple2<Double, Integer> merge(...) { ... }
```

## When to include tags

| Tag | Include when |
|-----|-------------|
| `@param` | Parameter name isn't self-explanatory, or there are multiple params that need distinction |
| `@return` | Return type alone doesn't tell the full story |
| `@throws` | Checked exceptions; unchecked only if callers should anticipate them |

**Skip** `@param`/`@return` for trivial getters/setters or when the signature fully speaks for itself. Don't pad with obvious tags just to look complete.

## Tone and Content

- Describe **what**, hint at **why** — not **how** (that's what the code is for)
- For Flink operators: describe the role in the streaming pipeline, not just the Java type (e.g., "computes the running average per key" rather than "implements AggregateFunction")
- Keep to 1–3 lines; use a blank `*` line to separate summary from body or tags
- Use `{@link ClassName}` when referencing related types inline

## Example — Before and After

**Before:**
```java
public class AggregateDemo {
    public static void main(String[] args) throws Exception { ... }

    private static class AvgWindowFunction
            implements AggregateFunction<ElectronicOrder, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.00, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(ElectronicOrder order, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + order.price, accumulator.f1 + 1);
        }
    }
}
```

**After:**
```java
/**
 * Flink streaming job that computes the average order price per electronic product
 * using a tumbling event-time window with an incremental AggregateFunction.
 */
public class AggregateDemo {

    /**
     * Builds the Flink pipeline, submits the job asynchronously, and waits for completion.
     */
    public static void main(String[] args) throws Exception { ... }

    /**
     * Incrementally accumulates sum and count of order prices within a window,
     * producing the running average as its result.
     */
    private static class AvgWindowFunction
            implements AggregateFunction<ElectronicOrder, Tuple2<Double, Integer>, Double> {

        /** Returns a zeroed (sum=0.0, count=0) accumulator. */
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.00, 0);
        }

        /**
         * Adds one order's price to the accumulator.
         *
         * @param order       incoming order record
         * @param accumulator running (sum, count) pair
         * @return updated accumulator
         */
        @Override
        public Tuple2<Double, Integer> add(ElectronicOrder order, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + order.price, accumulator.f1 + 1);
        }
    }
}
```
