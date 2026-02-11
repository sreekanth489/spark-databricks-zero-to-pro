# AI Functions on Databricks

> Module 21 -- Topic 04 | Level: Intermediate | Time: 50 min

## Learning Objectives

- Use `ai_query()` to call LLMs directly from SQL queries for data enrichment
- Apply built-in AI functions: `ai_classify()`, `ai_extract()`, `ai_summarize()`,
  `ai_translate()`, `ai_sentiment()`, and `ai_generate_text()`
- Design ETL pipelines that use AI functions to transform unstructured data
- Manage costs when running AI functions at scale
- Handle errors and edge cases in AI function pipelines

## Conceptual Overview

### What Are AI Functions?

AI Functions are SQL-native functions in Databricks that call LLMs behind the scenes.
They let data engineers and analysts use natural language processing directly in SQL
queries without writing Python code or managing model endpoints.

```
Traditional Pipeline:
  SQL Query --> Export Data --> Python Script --> LLM API --> Import Results --> SQL

AI Functions Pipeline:
  SQL Query with ai_query() --> Done (LLM call happens inline)
```

### Available AI Functions

```
+----------------------------------------------------------+
|                   AI Functions in SQL                     |
|                                                           |
|  +----------------+  +----------------+  +--------------+ |
|  | ai_query()     |  | ai_classify()  |  | ai_extract() | |
|  | General LLM    |  | Categorize     |  | Pull out     | |
|  | calls          |  | text           |  | entities     | |
|  +----------------+  +----------------+  +--------------+ |
|                                                           |
|  +----------------+  +----------------+  +--------------+ |
|  | ai_summarize() |  | ai_translate() |  | ai_sentiment | |
|  | Condense       |  | Convert        |  | Detect       | |
|  | text           |  | languages      |  | sentiment    | |
|  +----------------+  +----------------+  +--------------+ |
|                                                           |
|  +--------------------------------------------------+    |
|  | ai_generate_text()                                |    |
|  | Free-form text generation with custom prompts     |    |
|  +--------------------------------------------------+    |
+----------------------------------------------------------+
```

## ai_query() -- The Universal Function

`ai_query()` is the most flexible AI function. It calls any model endpoint with
a custom prompt and returns the response.

### Syntax

```sql
ai_query(endpoint, prompt [, returnType] [, modelParameters])
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| endpoint | STRING | Model serving endpoint name |
| prompt | STRING | The prompt text (can include column references) |
| returnType | STRING | Output type: `STRING`, `STRUCT<...>`, `ARRAY<STRING>` |
| modelParameters | MAP | `temperature`, `max_tokens`, `top_p`, `stop` |

### Examples

#### Simple Text Generation

```sql
SELECT ai_query(
  'databricks-dbrx-instruct',
  'Explain what a Delta table is in one sentence.'
) AS explanation
```

#### Column-Level Enrichment

```sql
SELECT
  product_id,
  description,
  ai_query(
    'databricks-dbrx-instruct',
    CONCAT('Classify this product description into one category ',
           '(Electronics, Clothing, Food, Home, Other): ', description),
    returnType => 'STRING'
  ) AS category
FROM products
```

#### Structured Output

```sql
SELECT
  email_id,
  ai_query(
    'databricks-dbrx-instruct',
    CONCAT('Extract the sender name, subject, and urgency level ',
           '(high/medium/low) from this email: ', email_body),
    returnType => 'STRUCT<sender: STRING, subject: STRING, urgency: STRING>'
  ) AS parsed
FROM emails
```

## ai_classify() -- Text Classification

Classifies text into one of the provided categories.

### Syntax

```sql
ai_classify(text, ARRAY('category1', 'category2', 'category3'))
```

### Examples

```sql
-- Classify support tickets
SELECT
  ticket_id,
  description,
  ai_classify(description, ARRAY('Billing', 'Technical', 'Account', 'General')) AS category
FROM support_tickets

-- Classify document types
SELECT
  doc_id,
  ai_classify(content, ARRAY('Contract', 'Invoice', 'Report', 'Memo', 'Email')) AS doc_type
FROM documents
```

## ai_extract() -- Entity Extraction

Extracts specified entities from text.

### Syntax

```sql
ai_extract(text, ARRAY('entity_type_1', 'entity_type_2'))
```

### Examples

```sql
-- Extract entities from customer feedback
SELECT
  feedback_id,
  ai_extract(
    feedback_text,
    ARRAY('product_name', 'issue_type', 'sentiment')
  ) AS extracted
FROM customer_feedback

-- Extract dates and amounts from invoices
SELECT
  invoice_id,
  ai_extract(
    invoice_text,
    ARRAY('invoice_date', 'due_date', 'total_amount', 'vendor_name')
  ) AS parsed
FROM raw_invoices
```

## ai_summarize() -- Text Summarization

Generates a concise summary of the input text.

### Syntax

```sql
ai_summarize(text)
```

### Examples

```sql
-- Summarize meeting notes
SELECT
  meeting_id,
  meeting_date,
  ai_summarize(transcript) AS summary
FROM meeting_transcripts

-- Summarize long articles
SELECT
  article_id,
  title,
  ai_summarize(body) AS abstract
FROM articles
WHERE length(body) > 5000
```

## ai_translate() -- Language Translation

Translates text from one language to another.

### Syntax

```sql
ai_translate(text, target_language)
```

### Examples

```sql
-- Translate product descriptions to Spanish
SELECT
  product_id,
  description,
  ai_translate(description, 'Spanish') AS description_es
FROM products

-- Multi-language support
SELECT
  message_id,
  content,
  ai_translate(content, 'French') AS content_fr,
  ai_translate(content, 'German') AS content_de,
  ai_translate(content, 'Japanese') AS content_ja
FROM customer_messages
```

## ai_sentiment() -- Sentiment Analysis

Detects the sentiment of text (positive, negative, neutral, mixed).

### Syntax

```sql
ai_sentiment(text)
```

### Examples

```sql
-- Analyze review sentiment
SELECT
  review_id,
  review_text,
  star_rating,
  ai_sentiment(review_text) AS detected_sentiment
FROM product_reviews
```

## ai_generate_text() -- Free-Form Generation

Generates text based on a custom prompt.

### Syntax

```sql
ai_generate_text(prompt)
```

### Example

```sql
-- Generate product descriptions
SELECT
  product_id,
  product_name,
  features,
  ai_generate_text(
    CONCAT('Write a compelling 2-sentence product description for: ',
           product_name, '. Features: ', features)
  ) AS generated_description
FROM products
```

## Using AI Functions in ETL Pipelines

### Pattern: Data Enrichment Pipeline

```
Raw Data (Delta)
     |
     v
+-------------------+
| AI Enrichment     |
| - classify        |
| - extract         |
| - sentiment       |
+-------------------+
     |
     v
Enriched Data (Delta)
     |
     v
Downstream Analytics
```

### Implementation

```sql
-- Full enrichment pipeline in a single SQL statement
CREATE OR REPLACE TABLE enriched_reviews AS
SELECT
  review_id,
  review_text,
  star_rating,
  ai_sentiment(review_text)                                    AS sentiment,
  ai_classify(review_text, ARRAY('Product', 'Service',
    'Shipping', 'Price', 'Quality'))                            AS topic,
  ai_extract(review_text, ARRAY('product_name', 'issue'))      AS entities,
  ai_summarize(review_text)                                     AS summary
FROM raw_reviews
```

## Cost Management

### Strategies

| Strategy | Description | Impact |
|----------|-------------|--------|
| Filter first | Apply WHERE clauses before AI functions | Reduces rows processed |
| Batch by priority | Process high-value data first | Controls spend |
| Cache results | Store AI outputs in Delta tables | Eliminates re-processing |
| Limit output | Set `max_tokens` in `modelParameters` | Reduces per-row cost |
| Sample test | Run on a sample before full dataset | Validates quality |

### Cost Estimation Query

```sql
-- Estimate cost before running
SELECT
  COUNT(*) AS total_rows,
  AVG(LENGTH(review_text)) / 4 AS avg_input_tokens,
  COUNT(*) * (AVG(LENGTH(review_text)) / 4 + 50) AS estimated_total_tokens,
  COUNT(*) * (AVG(LENGTH(review_text)) / 4 + 50) / 1000000 * 0.50 AS estimated_cost_usd
FROM raw_reviews
```

### Incremental Processing

```sql
-- Only process new rows (avoid re-processing)
INSERT INTO enriched_reviews
SELECT
  review_id,
  review_text,
  ai_sentiment(review_text) AS sentiment
FROM raw_reviews r
WHERE NOT EXISTS (
  SELECT 1 FROM enriched_reviews e WHERE e.review_id = r.review_id
)
```

## Error Handling

AI functions can fail due to rate limits, timeouts, or content filters.

```sql
-- Use TRY_* pattern for graceful error handling
SELECT
  review_id,
  COALESCE(
    TRY(ai_sentiment(review_text)),
    'UNKNOWN'
  ) AS sentiment
FROM reviews
```

## Key Takeaways

1. AI Functions bring LLM capabilities directly into SQL -- no Python needed
2. `ai_query()` is the most flexible, supporting any prompt and structured output
3. Specialized functions (`ai_classify`, `ai_sentiment`, etc.) are simpler for common tasks
4. Always estimate costs before running AI functions on large datasets
5. Use incremental processing to avoid re-enriching existing rows
6. Cache AI function outputs in Delta tables for downstream consumption

## Practice Exercises

1. Write a SQL query using `ai_classify()` to categorize news articles
2. Design an ETL pipeline using `ai_extract()` to parse invoice PDFs into a structured table
3. Calculate the cost of running `ai_sentiment()` on a table with 5 million rows

## Next Topic

[Topic 05: Fine-Tuning](05-fine-tuning.md)
