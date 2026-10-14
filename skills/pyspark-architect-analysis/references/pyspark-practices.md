# PySpark practices and pitfalls

## Policy: new code vs existing code

- **New code you write** follows the practices below — expressed in the file's own style (`style-profile.md`).
- **Existing code** is not changed to follow them. If an existing line violates a practice and that matters for the task, mention it under *Замечено попутно* with location and consequence. Fix only on request.
- A practice never overrides an established project convention that has a reason (e.g. the file writes intermediate results to temp tables and reads them back — keep doing that, it is how that project cuts lineage).

---

## 1. NULL semantics — the main source of silent row loss

| Code | What actually happens |
|---|---|
| `.where(F.col("x") != "A")` | rows with `x IS NULL` are **dropped** (NULL != 'A' is NULL, not true) |
| `.where(~F.col("x").isin(...))` | same — NULL rows dropped |
| `F.col("a") == F.col("b")` in a join | NULL keys never match |
| `F.when(cond, v)` without `.otherwise` | NULL for non-matching rows |
| `x / 0` | NULL (with ANSI off), no error |
| `F.sum`, `F.max`, `collect_set`, `countDistinct` | ignore NULLs; all-NULL group → NULL / empty / 0 |
| `F.concat(a, b)` | NULL if any part is NULL (`concat_ws` skips NULLs) |

In new code: decide explicitly what NULL should do. Use `F.coalesce(...)`, `.isNull()`/`.isNotNull()`, `eqNullSafe` for null-safe equality, `.otherwise(...)` when the else-branch matters. Reuse the file's sentinel values (`"-1"`, `0`, `1900-01-01`, `"(пусто)"` …) instead of inventing new ones.

## 2. Joins

- Before joining, know whether the right side is unique on the key. If not, the left side multiplies. If unsure — dedup the right side on the key **deterministically** (§4) or aggregate it first, and say so.
- `inner` drops unmatched rows from both sides; default `how` is inner. Use `left` unless dropping is intended, and state the intention.
- Ambiguous column names after a join: follow the file's convention — either rename with a suffix before the join (`id` → `id_`, `id_prev`) or `.alias("a")` + `F.col("a.x")`. Drop helper columns right after the join, as the file does.
- Join on a range/inequality condition (e.g. a calendar `between`) is a potential row explosion; estimate the multiplication factor in the plan.
- Small dimension tables: `F.broadcast(dim)` is fine in new code if the file already uses it or the table is clearly a small dictionary.

## 3. Aggregation and grain

- Every `groupBy` changes the grain. Name the grain before and after in the plan.
- Columns not in `groupBy` and not aggregated disappear — check they are not needed downstream (`--col`).
- `first()` / `last()` without a deterministic window order return an arbitrary row.

## 4. Deduplication

- `dropDuplicates()` on all columns only removes exact copies.
- `dropDuplicates([keys])` keeps an **arbitrary** row per key — results can differ between runs. In new code, when it matters which row survives:
  ```python
  w = Window.partitionBy("key").orderBy(F.col("updated_at").desc())
  df = df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
  ```

## 5. Windows

- `Window.orderBy(...)` without `partitionBy` moves all data to one partition — avoid in new code.
- With `orderBy`, the default frame is `unboundedPreceding .. currentRow`: `F.last(...)` returns the current row, not the partition's last. Specify `rowsBetween` explicitly.
- `row_number` over a non-unique order is non-deterministic — add a tie-breaker column.

## 6. Union

- `union` / `unionAll` match columns **by position**. Use `unionByName`.
- `unionByName(..., allowMissingColumns=True)` turns a typo in a column name into a new all-NULL column. When adding a branch to such a union, compare its `select` list with the other branches.

## 7. Time and dates

- Read the session time zone from the map (`НАСТРОЙКИ СЕССИИ`); it differs between files (`+03:00`, `Europe/Moscow`, UTC…).
- Reuse the file's existing conversion logic (offset columns, `unix_timestamp(...) - delta*3600`, period start/end variables) — do not introduce a second way to compute the same time.
- Python `datetime` objects passed into Spark (`F.lit(dt)`, comparisons with Python variables) are interpreted in the session time zone.
- `F.to_date(ts)` truncates in the session time zone.

## 8. Driver, loops and plan size

- `collect()` / `toPandas()` bring data to the driver. For "rows of A whose key is in B" use a join (`left_semi` / `left_anti`) instead of `isin(<collected list>)`.
- A Python `for`/`while` loop that reassigns a DataFrame grows the logical plan each iteration — prefer a window/aggregation solution; if a loop is unavoidable, write/read back or `checkpoint` periodically and say why.
- `.cache()` / `.persist()` only when a DataFrame is reused by several actions; pair with `.unpersist()`.
- `coalesce(1)` before a write funnels all data through one task — keep only where a single output file is required.

## 9. UDFs

Avoid Python UDFs in new code; nearly everything is expressible with `F.*`. If unavoidable, explain why and note the performance cost.

## 10. API version floor

The map prints `API: … → в файле уже используется Spark ≥ X`. In new code use only APIs at or below that floor (e.g. `withColumns` needs 3.3, `withColumnsRenamed`/`unpivot` 3.4). Need something newer → ask the user which Spark version runs on the cluster. Hints about the cluster version can also appear in saved outputs (e.g. Delta `history()` → `engineInfo`).

## 11. Writes

- Know the write pattern before touching the result: `overwrite` of the whole table; `delete(partitions) + append`; `merge`; `overwriteSchema=true` (schema can silently change for every consumer).
- Adding/removing/renaming a column in a written table changes its schema for every reader — list readers (other files) in the plan.
- Never change target table names, modes or partitioning as a side effect.

## 12. Readability in new code

- One logical step per assignment or chain; a short comment saying **why**, not what, in the file's language.
- Named intermediate DataFrames instead of one 80-line chain when a step has business meaning.
- Literal business values: reuse existing constants if the file defines them (UPPER_CASE names in the map's ПАРАМЕТРЫ И КОНСТАНТЫ).
