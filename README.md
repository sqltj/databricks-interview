# Databricks Interview Prep

A self-contained practice environment for Databricks Solutions Architect / Delivery SA technical interviews. Covers every major topic area with realistic scenario notebooks, a live interview template, synthetic data generators, and a serverless diagnostic reference — all deployable to **Databricks Free Edition** via Asset Bundles.

---

## Table of Contents

1. [What's in This Repo](#whats-in-this-repo)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
4. [Running the Practice Environment](#running-the-practice-environment)
5. [Scenario Reference](#scenario-reference)
6. [Interview Day Guide](#interview-day-guide)
7. [Serverless Diagnostic Toolkit](#serverless-diagnostic-toolkit)
8. [Study Guide](#study-guide)
9. [Interview Day Checklist](#interview-day-checklist)

---

## What's in This Repo

```
databricks-interview/
├── databricks.yml                        # Asset Bundle config (dev target → Free Edition)
├── resources/
│   └── interview_practice.job.yml        # 8-task job orchestrating all scenarios
├── src/notebooks/
│   ├── scratchpad.py                     # ← Use this on interview day (live template)
│   ├── 00_data_generator.py              # Synthetic data for 4 common domains
│   ├── 00_setup.py                       # Creates catalog + all schemas
│   ├── 01_delta_lake.py                  # Scenarios 1A–1C
│   ├── 02_spark_perf.py                  # Scenarios 2A–2E (serverless-optimized)
│   ├── 03_streaming.py                   # Scenarios 3A–3B
│   ├── 04_data_quality.py                # Scenarios 4A–4B
│   ├── 05_medallion.py                   # Scenarios 5A–5B
│   ├── 06_orchestration.py               # Scenario 6A
│   └── 99_teardown.py                    # Drops interview_practice catalog
└── docs/
    └── discovery_questions.md            # Phase 1 clarifying question guide
```

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| [Databricks Free Edition](https://www.databricks.com/try-databricks) | Sign up before the interview |
| [Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/install.html) | `brew install databricks` or `pip install databricks-cli` |
| Python 3.8+ | For running CLI and local tooling |
| Git | Clone and branch management |

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/sqltj/databricks-interview.git
cd databricks-interview
```

### 2. Configure the Databricks CLI

```bash
databricks configure --profile DEFAULT
# Enter your workspace URL: https://<your-workspace>.azuredatabricks.net
# Enter your personal access token
```

Verify it works:

```bash
databricks current-user me
```

### 3. Validate and deploy the Asset Bundle

```bash
databricks bundle validate     # check config is correct
databricks bundle deploy       # upload notebooks + create job in workspace
```

---

## Running the Practice Environment

### Run all scenarios (keeps data for review)

```bash
databricks bundle run interview_practice \
  --only setup,delta_lake,spark_perf,streaming,data_quality,medallion,orchestration
```

The `--only` flag skips `teardown` so tables persist after the run. Open the **Run URL** printed by the CLI to watch task progress.

### Run a single scenario

```bash
# Always run setup first if the catalog doesn't exist yet
databricks bundle run interview_practice --only setup
databricks bundle run interview_practice --only spark_perf
```

### Run everything including teardown (full cycle)

```bash
databricks bundle run interview_practice
```

### Clean up the workspace

```bash
databricks bundle run interview_practice --only teardown
# or destroy the whole bundle:
databricks bundle destroy
```

### Common bundle commands

```bash
databricks bundle validate          # lint and validate before deploy
databricks bundle deploy            # sync local changes to workspace
databricks bundle run <resource>    # trigger a job or pipeline run
```

---

## Scenario Reference

All scenarios follow the same structure:
- **Customer says:** the realistic problem statement you'd hear from a customer
- **Diagnosis:** how to identify the root cause (serverless-compatible)
- **Solution:** one or more fix options with trade-off explanations

### 1. Delta Lake (`01_delta_lake.py`)

| # | Problem | Key concepts |
|---|---------|-------------|
| **1A** | Schema evolution breaking downstream reads | `mergeSchema`, `DESCRIBE HISTORY`, column evolution |
| **1B** | Small files degrading query performance | `OPTIMIZE`, file compaction, Delta transaction log |
| **1C** | Accidental DELETE — time travel restore | `RESTORE TABLE`, `VERSION AS OF`, `TIMESTAMP AS OF` |

### 2. Spark Performance (`02_spark_perf.py`)

> All diagnostics in this notebook are **serverless-compatible** — no Spark UI required.

| # | Problem | Key concepts |
|---|---------|-------------|
| **2A** | Data skew — one task 2.5h, others seconds | Hash partitioning, AQE skew join, manual salting |
| **2B** | OOM / shuffle spill on large join | Sort-merge join mechanics, broadcast join, AQE coalesce |
| **2C** | Small files degrading scan performance | `DESCRIBE DETAIL`, `OPTIMIZE ZORDER BY`, bin-packing |
| **2D** | Wrong join strategy — SortMergeJoin on a 1k-row table | `explain("formatted")`, `broadcast()` hint, `ANALYZE TABLE` |
| **2E** | Full table scan despite a point filter | Delta file skipping, ZORDER vs PARTITION BY, column statistics |

### 3. Streaming (`03_streaming.py`)

| # | Problem | Key concepts |
|---|---------|-------------|
| **3A** | Late-arriving events missing from aggregations | Watermark algorithm, event time vs processing time, `append` output mode |
| **3B** | Checkpoint corruption causing duplicate reprocessing | Checkpoint contents (offsets + state), `foreachBatch` + `MERGE` idempotency |

### 4. Data Quality & Ingestion (`04_data_quality.py`)

| # | Problem | Key concepts |
|---|---------|-------------|
| **4A** | Auto Loader rejecting files after schema change | `cloudFiles.schemaEvolutionMode`, `_rescued_data`, schema inference |
| **4B** | Duplicate records from non-idempotent writes | `dropDuplicates`, `MERGE INTO`, natural keys |

### 5. Medallion Architecture (`05_medallion.py`)

| # | Problem | Key concepts |
|---|---------|-------------|
| **5A** | Design a Bronze/Silver/Gold clickstream pipeline | Layer responsibilities, Auto Loader → Delta, aggregation fan-out |
| **5B** | SCD Type 2 historical tracking | `MERGE INTO` with `WHEN MATCHED` / `WHEN NOT MATCHED`, effective dates |

### 6. Orchestration (`06_orchestration.py`)

| # | Problem | Key concepts |
|---|---------|-------------|
| **6A** | Pipeline produces incorrect data when restarted after failure | Idempotency, `MERGE` over `INSERT`, checkpoint strategies |

---

## Interview Day Guide

### Phase 1 — Discovery (first 5–10 minutes)

Before writing any code, open `docs/discovery_questions.md` and work through the structured question categories:

1. **Data volume & velocity** — sizes the compute approach
2. **Update patterns** — determines append vs. MERGE vs. CDC
3. **Data quality concerns** — sets up validation layer scope
4. **Downstream consumers** — determines Gold fan-out
5. **Failure & recovery** — surfaces idempotency requirements
6. **Access & security** — catches PII/masking requirements early

Close Phase 1 with a summary statement:

> *"So to confirm: we have ~X million records, arriving every Y minutes, with Z as the natural key. There may be duplicates, so I'll deduplicate in Silver. The downstream consumer is [X] with a [Y]-minute SLA. Does that match?"*

### Phase 2 — Building (use `scratchpad.py`)

`scratchpad.py` is your live interview canvas. It has:

- A **discovery notes table** — fill it in during Phase 1
- A **serverless diagnostic toolkit** — quick reference panel visible immediately on open
- **Bronze → Silver → Gold skeleton** with TODO placeholders and talking-point comments
- A **diagnostics cell** (row counts, DESCRIBE EXTENDED, DESCRIBE HISTORY)
- A commented-out **teardown cell**

**Workflow:**
1. Import or open `scratchpad.py` in your Databricks workspace before the interview
2. Fill in `CATALOG`, `SCHEMA`, `TABLE` after the prompt is given
3. Use `00_data_generator.py` to pick a domain template and adapt it
4. Work through Bronze → Silver → Gold, narrating each decision out loud

### Generating synthetic data (`00_data_generator.py`)

Four ready-made domain generators — pick the one closest to the interview prompt:

| Section | Domain | Built-in complexity |
|---------|--------|-------------------|
| **A** | E-commerce orders | 30% product skew, 2% null amounts, 8% returns |
| **B** | IoT sensor readings | 3% anomalous values, 5% late-arriving events |
| **C** | Financial transactions | 1% duplicate IDs, 25% hot merchant, 0.5% fraud |
| **D** | User clickstream | Exponential session durations, 30% null referrers |

Each generator is self-contained — run the section that matches your prompt, then reference the table in `scratchpad.py`.

---

## Serverless Diagnostic Toolkit

Databricks Free Edition uses **serverless compute only** — the Spark UI is not available. Use these instead:

| Goal | Command | Replaces |
|------|---------|---------|
| See query execution plan | `df.explain("formatted")` | DAG / Stages tab |
| Identify join strategy | Look for `BroadcastHashJoin` vs `SortMergeJoin` | Stages → shuffle size |
| Stage timings + shuffle bytes | Click **query profile bar** under cell output | Stages tab |
| File count and average size | `DESCRIBE DETAIL table` → `numFiles`, `sizeInBytes` | Storage UI |
| Diagnose data skew | `df.groupBy("key").count().orderBy(desc("count")).show(10)` | Task duration histogram |
| Confirm AQE is on | `spark.conf.get("spark.sql.adaptive.enabled")` | N/A |
| Rebuild column statistics | `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` | N/A |

**Mental model:** On classic clusters you tune at the *executor level* (heap, GC, task count). On serverless you tune at the *data level* — file sizes, key distribution, join strategy, query plan. The cluster manages itself.

### The 5 problems you can solve on serverless

| Problem | How to diagnose | Fix |
|---------|----------------|-----|
| Data skew | `groupBy(key).count()` — top key >> median | AQE (default on) or salting |
| Small files | `DESCRIBE DETAIL` — numFiles in thousands, avg < 32 MB | `OPTIMIZE ZORDER BY (col)` |
| Wrong join strategy | `explain()` shows `SortMergeJoin` on a small table | `broadcast()` hint |
| Full table scan | Query profile — Files Skipped = 0 | `OPTIMIZE ZORDER BY (filter_col)` |
| Bad query plans | Unexpected join order, no broadcast | `ANALYZE TABLE ... COMPUTE STATISTICS` |

---

## Study Guide

Work through the material in this order for maximum retention.

### Week 1 — Core Concepts

**Day 1–2: Delta Lake (Scenario 1A–1C)**
- Run `01_delta_lake.py` end-to-end
- Practice explaining `DESCRIBE HISTORY` output out loud
- Key talking point: *"Delta's transaction log is what makes time travel and schema evolution safe — every write is atomic and logged"*

**Day 3–4: Medallion Architecture (Scenario 5A–5B)**
- Run `05_medallion.py`
- Draw the Bronze → Silver → Gold diagram from memory
- Practice the layer responsibility explanation: *"Bronze = raw, never lose it. Silver = clean contract. Gold = business answer."*
- Understand SCD Type 2 MERGE pattern — this comes up frequently

**Day 5: Orchestration (Scenario 6A)**
- Understand why idempotency matters: safe to re-run = safe to schedule
- Know the difference between `INSERT` (not idempotent) and `MERGE` (idempotent)

### Week 2 — Performance & Streaming

**Day 1–2: Spark Performance (Scenarios 2A–2E)**
- Run all five scenarios in sequence
- For each one, practice the diagnosis step before looking at the solution
- Memorize the three tools: `explain("formatted")`, `DESCRIBE DETAIL`, query profile bar
- Practice the stakeholder translations out loud

**Day 3–4: Streaming (Scenarios 3A–3B)**
- Understand the watermark formula: `max(event_time) - delay`
- Know what a checkpoint stores: *offsets* (where to resume) + *state* (in-progress aggregations)
- Practice explaining why `foreachBatch + MERGE` prevents duplicates on restart

**Day 5: Data Quality (Scenarios 4A–4B)**
- Understand `cloudFiles.schemaEvolutionMode` options: `addNewColumns`, `rescue`, `failOnNewColumns`
- Know what `_rescued_data` is and why it matters

### Week 3 — Interview Simulation

**Day 1: Discovery practice**
- Read `docs/discovery_questions.md` until the question categories feel natural
- Practice asking questions in the right order: volume → updates → quality → consumers → failure
- Time yourself: aim to complete Phase 1 in under 7 minutes

**Day 2–3: Scratchpad practice**
- Pick a random domain from `00_data_generator.py`
- Set a timer for 45 minutes
- Complete a full Bronze → Silver → Gold pipeline in `scratchpad.py` while narrating out loud
- Repeat with a different domain

**Day 4: Perf diagnosis drills**
- Look at the data in each performance scenario table (not the solution code)
- Try to identify the problem and state a fix before scrolling to the solution

**Day 5: Full run**
- Deploy fresh, run the full job without teardown
- Review any failures in the Databricks job UI

### Key phrases to practice saying out loud

These signal strong engineering thinking to interviewers:

- *"My first instinct before writing any fix is to look at the data distribution..."*
- *"On serverless I can't see the Spark UI, but `explain()` shows me the physical plan..."*
- *"The reason this matters at scale is that each small file is a separate round-trip to object storage..."*
- *"Bronze never transforms — it's the replay buffer. If anything goes wrong downstream, we reprocess from Bronze."*
- *"MERGE makes this idempotent, meaning safe to run twice without producing duplicates."*
- *"The watermark is `max(event_time seen) minus the delay` — once a window's end is older than the watermark, it's finalized and the state is dropped."*

---

## Interview Day Checklist

**The night before:**
- [ ] Log in to Databricks Free Edition — confirm access
- [ ] Deploy the bundle: `databricks bundle deploy`
- [ ] Run setup: `databricks bundle run interview_practice --only setup`
- [ ] Open `scratchpad.py` in the workspace — confirm it renders correctly
- [ ] Open `00_data_generator.py` — note which section you'll reach for first
- [ ] Review `docs/discovery_questions.md` one more time

**Interview start (before the prompt):**
- [ ] Share your screen
- [ ] Have `scratchpad.py` open in one tab
- [ ] Have `00_data_generator.py` open in another tab
- [ ] Have `docs/discovery_questions.md` open for reference

**When you receive the prompt:**
- [ ] Fill in the discovery notes table in `scratchpad.py` as you ask questions
- [ ] Summarize what you heard before writing any code
- [ ] Set `CATALOG`, `SCHEMA`, `TABLE` variables
- [ ] Run the relevant data generator section, adapt as needed
- [ ] Build Bronze → Silver → Gold, narrating every decision

**During coding:**
- [ ] Think out loud — say why, not just what
- [ ] Call out trade-offs explicitly (*"I'm choosing MERGE over INSERT because..."*)
- [ ] Use the serverless toolkit reference in `scratchpad.py` when diagnosing perf
- [ ] If something fails, narrate your debugging approach before fixing it
