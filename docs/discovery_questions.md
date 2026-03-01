# Phase 1 Discovery Questions

Use these in the opening minutes of the interview. Strong candidates ask structured
questions that reveal engineering thinking — not just "what's the data?"

---

## 1. Data Volume & Velocity

> Shows you think about scale before writing a single line.

- How many records are we dealing with? Thousands, millions, billions?
- How often does new data arrive — real-time, micro-batch, daily batch?
- Is the volume consistent or does it spike? (e.g., end-of-month, flash sales)
- What's the historical retention window? Do we need years of data or just 90 days?

---

## 2. Update Patterns

> Determines whether you need append-only writes, MERGE/upsert, or CDC.

- Are records ever updated after they're written, or is this append-only?
- Are late-arriving records possible? How late can they be — minutes, hours, days?
- Is there a natural key I should use for deduplication?
- Do deletions need to be propagated downstream (e.g., GDPR right-to-erasure)?

---

## 3. Data Quality & Schema

> Shows you treat bad data as a first-class concern, not an afterthought.

- Are there known quality issues — nulls, duplicates, out-of-range values?
- Who owns the upstream schema? Can it change without notice?
- What should happen when a bad record arrives — reject, quarantine, or pass through?
- Are there validation rules we need to enforce (e.g., amount > 0, status must be in a set)?

---

## 4. Downstream Consumers

> A pipeline without a consumer is just a data dump.

- Who or what reads this data — a BI dashboard, an ML model, another pipeline, an API?
- What's the freshness SLA? Does the dashboard need data in 5 minutes or is T+1 fine?
- Are there multiple consumers with different needs? (This suggests Gold fan-out)
- Is there a schema contract we need to honor — e.g., a report that breaks if a column is renamed?

---

## 5. Failure & Recovery

> Resilience questions signal production-grade thinking.

- What happens if a pipeline run fails halfway through? Can it be re-run safely?
- Is idempotency required — can we run the same job twice without creating duplicates?
- Is there a monitoring/alerting expectation, or just "make it work"?
- How long can the pipeline be down before it causes a business impact?

---

## 6. Access & Security

> Easy to forget, always appreciated.

- Who needs access to the output — just data analysts, or also external systems?
- Is any of this data sensitive (PII, financial, health)? Does it need masking?
- Are there row-level security requirements (e.g., each region sees only its own data)?

---

## Quick Scoping Heuristic

After the answers, mentally bucket the work:

| Signal | Points toward |
|---|---|
| Append-only, high volume, near-real-time | Streaming + watermarks |
| Records updated frequently, natural key | MERGE/upsert, Delta ACID |
| Schema changes expected | `mergeSchema`, schema evolution |
| Bad data expected | Expectations / quarantine layer |
| Multiple downstream consumers | Medallion (Bronze → Silver → Gold fan-out) |
| One-time analysis | Skip the pipeline, just write Gold directly |

---

## Closing Phase 1

Before switching to code, summarize what you heard:

> "So to confirm: we have ~X million records, arriving every Y minutes, with Z as the
> natural key. There may be duplicates, so I'll deduplicate in Silver. The downstream
> consumer is [X] with a [Y]-minute SLA. Does that match your expectation?"

This shows you listened, caught the key constraints, and won't waste time on a wrong solution.
