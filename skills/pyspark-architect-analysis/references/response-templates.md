# Response templates

Locations: `L123` for .py, `c14` / `c14:6` for notebooks (cell number from the map, plus a short quote of the line). Keep answers as short as the task allows — the templates list what must be covered, not mandatory headings for a one-line fix.

## EXPLAIN

1. Назначение: what the file produces (written tables) and from what (sources), period/parameters.
2. Этапы: sections in order, one line each, with locations.
3. Ключевые DataFrame: name → role → grain; reassignments that matter.
4. Бизнес-правила: the rules as the code implements them (filters, categories from ЛИТЕРАЛЫ, formulas), each with location.
5. Инварианты и допущения: what must hold (time zone, sentinels, grain, uniqueness).
6. Риски: silent row loss points (inner joins, NULL filters, dedup), clones, protected zones relevant to the question.

## TRACE

1. Цель: column/value and the output where it lands (table, cell).
2. Цепочка (from output back to source), numbered:
   ```
   1. L1742 WRITE dds.sales_daily — колонка region_name
   2. L887  df_sales: region_name = coalesce(region_override, region_name)  ← подмена из df_overrides
   3. L884  join(df_dim_region, region_code == code, left)  — region_name приходит из dim_region
   4. L292  df_dim_region ← ref.dim_region (как есть)
   ```
   Mark filters, joins, aggregations, renames, `fillna`/sentinels along the way.
3. Источник: "значение берётся из … по правилу …".
4. Неопределённости: where several paths are possible (clones, union branches), with locations.

## DEBUG

1. Симптом as understood (entity, date, expected vs actual).
2. Где может теряться/меняться: ranked list with locations and why.
3. Диагностика: code blocks (`verify.md` templates) with what each result means.
4. Что прислать: exact outputs needed.
After the user's outputs: narrowed cause → proposed fix (then IMPLEMENT flow).

## IMPLEMENT

S-change:
1. Что меняю и где (location), clones and decision for each.
2. Diff (after the edit — show changed lines).
3. Проверка: static results + verification code for the user.
4. Impact summary (2–4 lines).

M/L-change — before editing:
1. Затронутая часть пайплайна (brief).
2. Точка внедрения + обоснование.
3. Чек-лист (from SKILL.md) filled in.
4. Варианты (L only) with trade-offs and recommendation.
5. План изменений: steps, files/cells, style profile.
6. Edge cases.
→ **stop and ask for confirmation.**

After confirmation: edit → static verification → verification code → Impact summary:
- изменённые DataFrame / ячейки / строки;
- downstream: что пересчитается, какие таблицы и их читатели затронуты;
- ожидаемое изменение числа строк (или "не меняется" + почему);
- что запустить и что должно получиться.

## Always, when applicable

**Замечено попутно** — problems outside the request (location, consequence, suggested fix), not fixed.
**Вопросы** — only those that block the work, one precise question each.
