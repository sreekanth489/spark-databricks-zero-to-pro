# Higher-Order Functions

> Module 04 -- Topic 06 | Level: Intermediate | Time: 35 min

## Learning Objectives

- Understand what higher-order functions are and why they exist
- Use TRANSFORM to apply a function to every element of an array
- Use FILTER to select array elements matching a condition
- Use AGGREGATE (REDUCE) to fold an array into a single value
- Apply EXISTS and FORALL for boolean checks on arrays
- Use ZIP_WITH and array_sort with custom comparators
- Explain why higher-order functions avoid UDF overhead

## Conceptual Overview

### The Problem: Operating on Array Elements

Suppose you have an array column `prices = [10.0, 25.0, 8.0, 42.0]` and you
want to apply a 10% discount to every element. You have three options:

1. **Explode, transform, re-aggregate** -- works but verbose and causes a
   shuffle (explode creates new rows, then you groupBy to reassemble)
2. **Python UDF** -- flexible but slow (Pickle serialization)
3. **Higher-order functions** -- run natively in Catalyst, no serialization
   overhead, no explode needed

Higher-order functions are the clear winner for array manipulations.

### How Higher-Order Functions Avoid UDF Overhead

Built-in higher-order functions like TRANSFORM, FILTER, and AGGREGATE are
executed entirely inside the JVM by the Catalyst optimizer. There is no data
transfer between JVM and Python, no Pickle, no Arrow -- just native execution:

```
  Python UDF path:
  JVM --> Pickle --> Python --> Pickle --> JVM  (slow)

  Higher-order function path:
  JVM --> Catalyst expression --> JVM  (fast, zero serialization)
```

This makes them significantly faster than even Pandas UDFs for array operations.

### TRANSFORM -- Apply to Every Element

Applies a lambda function to each element of an array, returning a new array
of the same length:

```
  TRANSFORM(array, x -> expression)

  prices = [10, 25, 8, 42]

  TRANSFORM(prices, x -> x * 0.9)   =>  [9.0, 22.5, 7.2, 37.8]
  TRANSFORM(prices, x -> x + 5)     =>  [15, 30, 13, 47]
```

```
  Before:  [10, 25,  8, 42]
             |   |   |   |
  Lambda:  *0.9 *0.9 *0.9 *0.9
             |   |   |   |
  After:   [9.0, 22.5, 7.2, 37.8]
```

### FILTER -- Select Matching Elements

Returns a new array containing only elements that satisfy the predicate:

```
  FILTER(array, x -> condition)

  prices = [10, 25, 8, 42]

  FILTER(prices, x -> x > 15)   =>  [25, 42]
  FILTER(prices, x -> x < 10)   =>  [8]
```

### AGGREGATE (REDUCE) -- Fold Into a Single Value

Reduces an array to a single value by iteratively applying a merge function:

```
  AGGREGATE(array, initial_value, (accumulator, element) -> merge, finish)

  prices = [10, 25, 8, 42]

  AGGREGATE(prices, 0, (acc, x) -> acc + x)   =>  85  (sum)
  AGGREGATE(prices, 1, (acc, x) -> acc * x)   =>  84000  (product)
```

Step-by-step for sum:

```
  Start:  acc = 0
  Step 1: acc = 0 + 10  = 10
  Step 2: acc = 10 + 25 = 35
  Step 3: acc = 35 + 8  = 43
  Step 4: acc = 43 + 42 = 85
  Result: 85
```

### EXISTS and FORALL -- Boolean Checks

```
  EXISTS(array, x -> condition)    -- true if ANY element matches
  FORALL(array, x -> condition)    -- true if ALL elements match

  prices = [10, 25, 8, 42]

  EXISTS(prices, x -> x > 40)   =>  true   (42 > 40)
  FORALL(prices, x -> x > 5)    =>  true   (all > 5)
  FORALL(prices, x -> x > 10)   =>  false  (8 is not > 10)
```

### ZIP_WITH -- Combine Two Arrays Element-Wise

```
  ZIP_WITH(array1, array2, (a, b) -> expression)

  names = ["laptop", "phone"]
  prices = [1200, 800]

  ZIP_WITH(names, prices, (n, p) -> concat(n, ': $', p))
    => ["laptop: $1200", "phone: $800"]
```

### array_sort with Custom Comparator

Sort array elements using a custom comparison function:

```python
# Sort strings by length (shortest first)
array_sort(col("items"), lambda a, b:
    when(length(a) < length(b), lit(-1))
    .when(length(a) > length(b), lit(1))
    .otherwise(lit(0))
)
```

### Comparison: Higher-Order Functions vs Alternatives

```
  +----------------------------+------------------+------------------+
  | Approach                   | Speed            | Complexity       |
  +----------------------------+------------------+------------------+
  | Higher-order functions     | Fastest (JVM)    | Low              |
  | Explode + groupBy          | Moderate (shuffle)| Medium          |
  | Pandas UDF on arrays       | Moderate (Arrow) | Medium           |
  | Python UDF on arrays       | Slowest (Pickle) | Low              |
  +----------------------------+------------------+------------------+
```

## Hands-On Walkthrough

Open the companion notebook `06-higher-order-functions_notebook.py` which covers:

1. E-commerce data with array columns (item prices, tags)
2. TRANSFORM to apply discounts to price arrays
3. FILTER to extract items above a threshold
4. AGGREGATE to compute totals and averages within arrays
5. EXISTS and FORALL for order validation
6. ZIP_WITH to combine item names and prices
7. array_sort with a custom comparator
8. Comparison with explode approach (to show higher-order is simpler)
9. SQL syntax for all higher-order functions

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|---------------------|----------------|
| TRANSFORM, FILTER | Spark 2.4+ | All DBR | Spark 2.4+ |
| AGGREGATE | Spark 2.4+ | All DBR | Spark 2.4+ |
| EXISTS, FORALL | Spark 3.0+ | DBR 7.0+ | Spark 3.0+ |
| array_sort with comparator | Spark 3.0+ | DBR 7.0+ | Spark 3.0+ |
| Photon acceleration | N/A | HOFs run in Photon | N/A |

## Certification Tip

Higher-order functions commonly appear in Databricks exam questions:
- Know the syntax: `TRANSFORM(array, x -> x * 2)`
- Understand that these run in the JVM, not in Python
- Know that FILTER returns a subset, TRANSFORM returns same-size array
- Be able to distinguish AGGREGATE (fold) from regular aggregation functions
- Remember that EXISTS checks "any" and FORALL checks "all"

## Key Takeaways

1. Higher-order functions operate on arrays natively in the Catalyst engine.
2. No serialization overhead -- unlike UDFs, no data leaves the JVM.
3. TRANSFORM applies a function to every element (map operation).
4. FILTER selects elements matching a predicate.
5. AGGREGATE (REDUCE) folds an array into a single value.
6. EXISTS returns true if any element matches; FORALL requires all to match.
7. ZIP_WITH combines two arrays element-wise with a merge function.
8. Always prefer higher-order functions over UDFs for array transformations.

## Next Steps

Proceed to **Topic 07 -- Data Modeling Patterns** to learn how to organize
your transformations into production data models using managed/external tables,
star schemas, and the medallion architecture.
