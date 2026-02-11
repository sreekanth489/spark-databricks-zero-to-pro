# Spark & Databricks: Zero to Pro

A comprehensive, hands-on learning path covering **Apache Spark** and the **Databricks Lakehouse Platform** — from absolute beginner to certification-ready professional.

Every topic includes a **conceptual guide** (`.md`) and a **runnable Databricks notebook** (`_notebook.py`) that you can import directly into any Databricks workspace (including the free Community Edition).

---

## Learning Roadmap

```
Phase 1 – Foundations          Phase 2 – Core Engineering       Phase 3 – Advanced
┌──────────────────────┐      ┌──────────────────────────┐     ┌──────────────────────────┐
│ 00 Setup & Basics    │─────▶│ 04 Transformations &     │────▶│ 07 Streaming & Real-Time │
│ 01 Python & Spark    │      │    Data Modeling         │     │ 08 Governance & Security │
│ 02 Data Ingestion    │      │ 05 Performance           │     │ 09 Testing & Monitoring  │
│ 03 Delta Lake &      │      │    Optimization          │     └──────────────────────────┘
│    Lakehouse         │      │ 06 Orchestration & CI/CD │              │
└──────────────────────┘      └──────────────────────────┘              ▼
                                                              Phase 4 – Capstone
                                                              ┌──────────────────────────┐
          Phase 5 – AI / ML Track                             │ 10 Real-World Projects   │
          ┌──────────────────────────┐                        │ 11 Certification Prep    │
          │ 20 ML & AI Foundations   │                        └──────────────────────────┘
          │ 21 GenAI & LLM Use Cases │
          │ 22 Agentic AI            │
          └──────────────────────────┘
```

---

## Module Index

### Data Engineering Track

| Module | Topic | Level | Est. Time |
|--------|-------|-------|-----------|
| [00-setup-basics](00-setup-basics/) | Databricks workspace, clusters, notebooks, DBFS | Beginner | 3–4 hrs |
| [01-python-spark-foundations](01-python-spark-foundations/) | Python essentials, Spark architecture, RDDs, DataFrames, SQL | Beginner | 6–8 hrs |
| [02-data-ingestion](02-data-ingestion/) | File readers, Auto Loader, COPY INTO, external sources | Beginner–Intermediate | 4–5 hrs |
| [03-delta-lake-lakehouse](03-delta-lake-lakehouse/) | Delta Lake, ACID, time travel, schema evolution, medallion | Intermediate | 6–8 hrs |
| [04-transformations-modeling](04-transformations-modeling/) | Joins, windows, UDFs, complex types, data modeling | Intermediate | 6–8 hrs |
| [05-performance-optimization](05-performance-optimization/) | Spark UI, partitioning, caching, AQE, Photon | Intermediate–Advanced | 6–8 hrs |
| [06-orchestration-ci-cd](06-orchestration-ci-cd/) | Workflows, DLT, Asset Bundles, CI/CD, Databricks Connect | Intermediate–Advanced | 4–5 hrs |
| [07-streaming-realtime](07-streaming-realtime/) | Structured Streaming, triggers, watermarks, Kafka | Advanced | 5–6 hrs |
| [08-governance-security](08-governance-security/) | Unity Catalog, lineage, row/column security, federation | Advanced | 5–6 hrs |
| [09-testing-monitoring](09-testing-monitoring/) | Testing Spark code, data quality, monitoring, cost mgmt | Advanced | 4–5 hrs |
| [10-real-world-projects](10-real-world-projects/) | End-to-end pipelines: e-commerce, IoT, CDC | Capstone | 8–12 hrs |
| [11-certification-prep](11-certification-prep/) | Databricks Associate & Professional exam prep | All levels | 6–10 hrs |

### AI / ML Track

| Module | Topic | Level | Est. Time |
|--------|-------|-------|-----------|
| [20-ml-ai-foundations](20-ml-ai-foundations/) | MLflow, Feature Store, AutoML, Model Serving | Intermediate | 6–8 hrs |
| [21-genai-llm-usecases](21-genai-llm-usecases/) | Foundation Models, RAG, Vector Search, AI Functions | Intermediate–Advanced | 5–6 hrs |
| [22-agentic-ai-databricks](22-agentic-ai-databricks/) | Agent Framework, tool calling, multi-agent systems | Advanced | 4–5 hrs |

---

## Quick Start

### 1. Set up a Databricks workspace

Use the free **Community Edition** — no cloud account required.
See [docs/setup-community-edition.md](docs/setup-community-edition.md) for step-by-step instructions.

### 2. Import notebooks

Each `_notebook.py` file uses [Databricks source format](https://docs.databricks.com/en/notebooks/notebook-format.html) and can be imported directly:

1. In your workspace, click **Import** in the sidebar
2. Choose **File** and upload any `_notebook.py` file
3. The notebook opens ready to run — all sample data is generated in-cell

See [docs/importing-notebooks.md](docs/importing-notebooks.md) for detailed instructions.

### 3. Start learning

Begin with [Module 00](00-setup-basics/) and follow the roadmap, or jump to any module that matches your level.

---

## Certification Mapping

Content maps to two Databricks certifications:

| Certification | Modules |
|---------------|---------|
| **Databricks Certified Data Engineer Associate** | 00–06, 08 |
| **Databricks Certified Data Engineer Professional** | 03–09 |

Each guide includes a "Certification Tip" section highlighting exam-relevant concepts.

---

## Repository Structure

```
spark-databricks-zero-to-pro/
├── 00-setup-basics/               # Module directories
│   ├── README.md                  #   Module overview & table of contents
│   ├── 01-topic-name.md           #   Conceptual guide
│   └── 01-topic-name_notebook.py  #   Runnable Databricks notebook
├── ...
├── docs/                          # Setup guides, glossary
│   ├── setup-community-edition.md
│   ├── importing-notebooks.md
│   └── glossary.md
├── resources/
│   └── data-generators/           # Shared data generation utilities
│       ├── generator_utils.py
│       └── generate_ecommerce.py
├── CLAUDE.md                      # AI assistant conventions
├── .gitignore
└── README.md                      # This file
```

---

## Design Principles

- **Self-contained notebooks** — every notebook generates its own sample data; no dependency on prior notebooks
- **Cloud-agnostic** — generic code with AWS / Azure / GCP difference tables where relevant
- **Zero external dependencies** — only libraries in Databricks Runtime (PySpark, pandas, numpy)
- **Progressive difficulty** — within and across modules, from beginner to advanced
- **Certification-aligned** — every guide maps to Databricks exam domains

---

## Contributing

1. Follow the naming conventions in [CLAUDE.md](CLAUDE.md)
2. Every topic needs both a `.md` guide and a `_notebook.py` notebook
3. Notebooks must use Databricks source format and be self-contained
4. Test notebooks on Databricks Community Edition before submitting

## License

This project is for educational purposes.
