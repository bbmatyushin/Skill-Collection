# Verification

You never run Spark code. Verification has two parts: static checks you run yourself, and verification code the user runs.

## A. Static checks (you run them)

Before the edit, save the map:
```bash
python3 <skill_dir>/scripts/pipeline_index.py FILE --full > DIR/map_before.txt
```
After the edit:
```bash
python3 <skill_dir>/scripts/pipeline_index.py FILE --check          # syntax; for .ipynb also JSON structure and forgotten %%spark
python3 <skill_dir>/scripts/pipeline_index.py FILE --full > DIR/map_after.txt
diff DIR/map_before.txt DIR/map_after.txt
```
Check the diff contains **only** what you intended: no new/removed table writes, no changed write modes, no unexpected DataFrame or column changes. Line numbers shift below an insertion in .py — that alone is fine.

For every column your new code reads, run `--col NAME` and confirm it exists in the DataFrame state at your insertion point (set earlier, not dropped/renamed in between). For every DataFrame you use, `--df NAME` confirms you use the right reassignment.

`--check` exit code 1 = must fix before reporting done.

## B. Verification code for the user

Give it in the chat by default (the user copies it into a scratch cell). Insert into the notebook only if asked — then as separate cells **after the barrier** in a clearly named section ("Проверка правки <дата>").

Rules for verification code:
- Read-only: no `saveAsTable`, `insertInto`, `delete`, `merge`, `overwrite`, no `%%configure`. If a comparison needs persistence, write only to a `temp.` table the user approves, never to targets.
- Notebook: start each cell with the same magic as pipeline cells (`%%spark`). Script: say where to run it (a scratch notebook against the same session/period).
- Small outputs: `.show(n, truncate=False)` with small `n`, aggregates instead of raw rows; any `-o` transfer only after `.limit()`.
- Each block: what it checks → what result is expected → what a deviation means.

### Templates

Row counts across the changed stage:
```python
for name, d in [("до", df_before), ("после", df_after)]:
    print(name, d.count())
```

Key uniqueness on the right side of a join (multiplication risk):
```python
(df_right.groupBy("key").count().where(F.col("count") > 1)
    .orderBy(F.desc("count")).show(10, truncate=False))
```

Join match rate (how many rows find a pair):
```python
(df_left.join(df_right.select("key").distinct().withColumn("_hit", F.lit(1)), "key", "left")
    .groupBy(F.col("_hit").isNotNull().alias("matched")).count().show())
```

Distribution of a new/changed column:
```python
df.groupBy("new_col").count().orderBy(F.desc("count")).show(20, truncate=False)
df.select(F.count(F.when(F.col("new_col").isNull(), 1)).alias("nulls"), F.count("*").alias("rows")).show()
```

Old vs new logic side by side, without writing anything:
```python
cmp = (df.withColumn("old_val", <old expression>)
         .withColumn("new_val", <new expression>)
         .where(~F.col("old_val").eqNullSafe(F.col("new_val"))))
print("отличается строк:", cmp.count())
cmp.select("key", "old_val", "new_val").show(20, truncate=False)
```

Trace one entity through stages (DEBUG):
```python
KEY, DAY = "<id сущности>", "<дата>"
for name, d in [("1. источник", df_src), ("2. после фильтра", df_filtered), ("3. после join", df_joined)]:
    print(name)
    d.where((F.col("<ключ>") == KEY) & (F.to_date("<дата-колонка>") == DAY)).select(<нужные колонки>).show(truncate=False)
```

Totals before/after for a written metric (on the same period):
```python
df_new.groupBy("date").agg(F.sum("metric").alias("new")).join(
    spark.table("<target>").where(<same period>).groupBy("date").agg(F.sum("metric").alias("old")), "date", "full"
).orderBy("date").show(50)
```

### What to ask the user to paste back

Name the block and say exactly: "пришлите вывод целиком, как есть" — plus what you will conclude from each possible result. Prefer outputs that fit in one screen.
