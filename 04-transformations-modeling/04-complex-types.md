# Complex Types

> Module 04 -- Topic 04 | Level: Intermediate | Time: 40 min

## Learning Objectives

- Work with Spark's three complex types: Array, Map, and Struct
- Parse and flatten nested JSON structures
- Use explode and posexplode to convert arrays into rows
- Apply array functions: array_contains, array_distinct, flatten, zip_with
- Access and transform map and struct fields
- Use schema_of_json, from_json, and to_json for JSON processing

## Conceptual Overview

### Why Complex Types Matter

Real-world data is rarely flat. API responses contain nested objects, event
streams embed arrays of actions, and configuration data uses key-value maps.
Spark provides first-class support for nested data through three complex types:

```
  +------------------------------+------------------------------------+
  | Type     | Example           | Description                        |
  +------------------------------+------------------------------------+
  | Array    | [1, 2, 3]         | Ordered collection of same-type    |
  |          |                   | elements                           |
  +------------------------------+------------------------------------+
  | Map      | {a: 1, b: 2}     | Key-value pairs (keys must be      |
  |          |                   | same type, values same type)       |
  +------------------------------+------------------------------------+
  | Struct   | {name: "Jo",      | Named fields of potentially        |
  |          |  age: 30}         | different types (like a row)       |
  +------------------------------+------------------------------------+
```

### Nested JSON -- The Common Scenario

Consider a web analytics event:

```json
{
  "user_id": "U001",
  "event": "page_view",
  "timestamp": "2024-01-15T10:30:00Z",
  "properties": {
    "page": "/products",
    "referrer": "google.com",
    "tags": ["electronics", "sale"]
  }
}
```

In Spark, this maps to:

```
root
 |-- user_id: string
 |-- event: string
 |-- timestamp: string
 |-- properties: struct
 |    |-- page: string
 |    |-- referrer: string
 |    |-- tags: array<string>
```

### Explode -- Arrays to Rows

The `explode()` function converts each element of an array into a separate row:

```
  BEFORE explode                      AFTER explode("tags")

  user_id  tags                       user_id  tag
  -------  ----                       -------  ---
  U001     [elec, sale]               U001     elec
                                      U001     sale
  U002     [books]                    U002     books
  U003     [elec, books, sale]        U003     elec
                                      U003     books
                                      U003     sale
```

`posexplode` also returns the position index:

```
  user_id  pos  tag
  -------  ---  ---
  U001     0    elec
  U001     1    sale
```

### Array Functions

Spark provides a rich set of array manipulation functions:

```
  array_contains(arr, val)   -- returns true if val is in the array
  array_distinct(arr)        -- removes duplicate elements
  array_union(a, b)          -- union of two arrays
  array_intersect(a, b)      -- intersection of two arrays
  array_except(a, b)         -- elements in a but not in b
  flatten(arr_of_arr)        -- flattens nested arrays into one
  sort_array(arr)            -- sorts elements in ascending order
  size(arr)                  -- number of elements
  zip_with(a, b, func)      -- combines two arrays element-wise
  slice(arr, start, len)     -- extracts a sub-array
```

### Map Functions

```
  map_keys(m)                -- returns array of keys
  map_values(m)              -- returns array of values
  map_from_entries(arr)      -- creates map from array of (k,v) pairs
  element_at(m, key)         -- retrieves value for a key
  map_concat(m1, m2)         -- merges two maps
  explode(m)                 -- produces (key, value) rows
```

### Struct Access

Access struct fields using dot notation:

```python
df.select("properties.page", "properties.referrer")
```

Or use `col("properties").getField("page")` for programmatic access.

### JSON Processing Pipeline

```
  Raw JSON string
       |
       v
  schema_of_json()          -- infer schema from a sample
       |
       v
  from_json(col, schema)    -- parse string into struct
       |
       v
  Access nested fields       -- dot notation or getField
       |
       v
  to_json(col)              -- convert struct back to JSON string
```

## Hands-On Walkthrough

Open the companion notebook `04-complex-types_notebook.py` which covers:

1. Creating sample data with arrays, maps, and structs
2. Accessing struct fields with dot notation
3. Explode and posexplode on arrays
4. Array functions: contains, distinct, union, intersect, flatten
5. Map operations: keys, values, element_at, explode
6. Parsing raw JSON strings with schema_of_json and from_json
7. Converting back to JSON with to_json
8. Nested struct flattening for analytics
9. zip_with for element-wise array operations

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|---------------------|----------------|
| schema_of_json | Spark 2.4+ | All DBR | Spark 2.4+ |
| zip_with | Spark 2.4+ | All DBR | Spark 2.4+ |
| Nested column pruning | Spark 3.0+ | Enabled by default on DBR | Spark 3.0+ |
| JSON auto-schema detection | Manual | Auto Loader w/ schema inference | Manual |
| Photon nested type perf | N/A | Accelerated in Photon | N/A |

## Certification Tip

The Databricks certification frequently tests:
- Using `explode` to flatten array columns
- Dot notation to access struct fields (e.g., `properties.page`)
- Knowing that `from_json` requires a schema and returns a struct
- Understanding that `size()` returns the number of elements in an array

## Key Takeaways

1. Spark supports three complex types: Array, Map, and Struct.
2. `explode()` converts array elements into rows; `posexplode()` adds position.
3. Array functions like `array_contains`, `array_distinct`, and `flatten`
   operate natively without UDF overhead.
4. Maps are accessed with `element_at()` or exploded into key-value rows.
5. Struct fields use dot notation for access.
6. Use `from_json` with a schema to parse JSON strings into structured data.
7. `to_json` converts structured data back into JSON strings.
8. Always check for nulls when exploding -- `explode` drops rows with null or
   empty arrays. Use `explode_outer` to preserve them.

## Next Steps

Proceed to **Topic 05 -- UDFs and Pandas UDFs** to learn how to extend Spark
with custom Python functions when built-in functions are not enough.
