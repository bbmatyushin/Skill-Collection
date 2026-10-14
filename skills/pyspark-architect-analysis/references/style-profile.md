# Style profile

Large PySpark files usually contain several styles at once: old code and newer changes written differently. "Follow the existing style" means a concrete procedure, not a general wish.

## 1. Collect evidence

From the map, section **СТИЛЬ**:
- chain style in the whole file and in its last third (`'\'` continuations vs parenthesized chains);
- `F.col(` vs bare `col(`, double vs single quotes;
- `withColumns` vs chained `withColumn`, `.alias()` joins vs rename-with-suffix joins;
- comment language;
- author tags (`-- name`) and change markers (`## UPDATE 1.2 2026-01-04 -- name` … `## UPDATE 1.2 END`, `# Доработка 6`, `# NEW v4: …`).

Then read two places in the code:
- **the block you are going to change** (and its neighbours);
- **the most recent change blocks** — the last change markers listed in the map.

## 2. Write down the profile (in the plan, 3–6 lines)

```
Стиль: правка внутри цепочки L700–L713 — цепочка через '\', F.col, двойные кавычки.
Новые блоки в файле: в скобках, комментарии на русском "почему", маркеры
"## UPDATE N.M YYYY-MM-DD -- author" … "## UPDATE N.M END".
Join с неоднозначными колонками: переименование с суффиксом "_" и drop после join.
```

## 3. Apply

- **Edit inside an existing chain/statement** → keep that statement's style exactly (backslashes stay backslashes, indentation stays). Do not convert the statement to another style.
- **New block** → write it the way the most recent change blocks are written (chain style, comment manner, markers).
- **Change markers**: if the file has a convention, wrap your change in it — with today's date, and the author tag the user uses (ask once if there are several, never put your own name). If the file has no convention, add no markers.
- **Notebooks**: also match cell granularity (one logical step per cell vs long cells) and markdown headings (e.g. numbered `##### 5. …` steps). New cells start with the same magic as neighbouring pipeline cells.
- Naming: same patterns as the file (e.g. `df_<entity>`, a suffix for grouped helpers, a suffix for renamed join copies, UPPER_CASE constants).
- Comments: same language; explain business reasons ("плановые даты могут быть пустыми"), not syntax.

## 4. Do not

- Reformat, re-indent, or re-quote lines you did not need to change.
- "Modernize" old code (backslashes → parentheses, `withColumn` chains → `withColumns`) as part of a feature change.
- Remove commented-out code, debug `.show()` lines or old markers.
- Change the order of existing columns in `select` lists unless required.

## 5. Examples

Edit inside an old-style chain — keep it (the third `.where` is the new line; note that a comment cannot follow a `\` continuation, so explain the change in the answer or on a separate line above the statement):
```python
    df_orders = df_orders\
        .where(F.col("is_active") == 1)\
        .where(F.col("region").isNotNull())\
        .where(F.col("order_type").isNotNull())\
        .join(df_dim_region, F.col("region") == F.col("region_code"), "left")\
```

New block in a file whose recent changes use parentheses and markers:
```python
    ## UPDATE 2.1 2026-10-14    -- author
    # Исключаем заказы без типа: они не должны попадать в итоговые метрики
    df_orders = (
        df_orders
        .where(F.col("order_type").isNotNull())
    )
    ## UPDATE 2.1 END
```
