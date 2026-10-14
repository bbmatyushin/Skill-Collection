# Notebooks: Jupyter + sparkmagic (Livy), VS Code

## How such a notebook executes

- The kernel is a **local** Python (`kernelspec` like `python3`). `%reload_ext sparkmagic.magics` + `%manage_spark` create a remote Livy session.
- A cell runs **on the cluster only if it starts with `%%spark`** (or `%%sql`). A cell without a magic runs **locally**: it does not see `spark` or any remote DataFrame, and the cluster does not see its variables.
- If the kernel itself is a Spark kernel (`pysparkkernel`), cells without a magic are remote and `%%local` marks local ones. The map prints which case applies: `ячейки без магика выполняются: LOCAL|REMOTE`.
- `spark` already exists in the session. There is no `SparkSession.builder` and no `argparse`; parameters are constants in early cells.

## Cell kinds (map → ЯЧЕЙКИ)

| Kind | Recognized by | Meaning for you |
|---|---|---|
| REMOTE | `%%spark` (or no magic in a Spark kernel) | pipeline code |
| SQL | `%%sql`, `%%spark -c sql` | pipeline SQL; tables parsed from text |
| LOCAL | no magic in a python kernel, `%%local` | local helpers, notes, guards; **not** pipeline |
| DISABLED | `# %%spark` (magic commented out) | switched-off cell; it does nothing |
| BARRIER | cell that is only `raise` / `assert False` / `exit()` | stops "Run All": everything below is manual tooling |
| CONFIG | `%%configure -f {...}` | session resources/conf; restarts the session — protected |
| SESSION | `%manage_spark`, `%reload_ext`, `%%info`, `%%logs`, `%%cleanup`, `%%delete` | session management — ignore for logic |
| RUN | `%run ./other` | imports another notebook's definitions — follow it |

Transfers: `%%spark -o df_name` / `%%sql -o name` pull a remote result into a local pandas DataFrame (default sample ~2500 rows). Treat as a driver collect.

## Execution order and hidden state

- Reconstruct the logic **top to bottom over pipeline cells before the barrier**. That is how "Run All" or a scheduler runs it.
- The Livy session is long-lived. Saved state can come from cells that were edited, deleted, run out of order or never run. The map warns about cells without `execution_count` between executed ones and about out-of-order runs.
- Cells after the barrier (sections like "Tests", "Restore", "Проверка") are manual tools. They may contain destructive operations (`restoreToVersion`, `delete`, `overwrite`). Never treat them as pipeline logic, never edit or move them unless asked.

## Saved outputs

- `--emit DIR` writes `<name>.outputs.txt`: stream/error/text outputs without widgets, truncated.
- Outputs are real data and the only data you can see without the user — use them (schemas from `printSchema`, samples from `.show()`, printed date ranges, Spark version from Delta `history()`).
- Always label conclusions from outputs as *"по сохранённому выводу c60, может не соответствовать текущему коду"*, especially when the map warns about execution state.

## Reading

- Read `<name>.cells.py` with `Read offset/limit` using the `v:` ranges from ЯЧЕЙКИ. Each cell starts with a header `# %% [c14 REMOTE id=40046ff0 ex=60]`.
- Address cells to the user as `c14` (number, 0-based as in the map) plus a short quote; use the `id` only for tools — numbers shift when cells are inserted, ids do not.

## Editing with NotebookEdit

1. **Before editing, ask the user to save the notebook in VS Code** (and preferably close it). VS Code keeps its own in-memory copy: saving an open, dirty notebook after your edit overwrites your change, or shows a conflict. After the edit, tell the user to reload the notebook from disk ("Revert File" / reopen).
2. `NotebookEdit` requires a `Read` of the notebook in this conversation. Do one `Read` of the `.ipynb` right before the first edit — for that requirement, not for analysis.
3. **Replace** (`edit_mode=replace`, `cell_id`): `new_source` is the whole cell. Copy the current cell text from `.cells.py` exactly (without the `# %% [...]` header), change only the needed lines, keep the magic first line (`%%spark`) and the blank line after it if present.
4. **Insert** (`edit_mode=insert`, `cell_id` = the cell after which to insert, `cell_type=code|markdown`): new code cells start with the same magic as neighbouring pipeline cells. Insert before the barrier. If the notebook uses numbered markdown step headings, add/adjust the heading the same way.
5. Never delete cells, clear outputs, or touch other cells' `execution_count`.
6. Never edit the JSON with `Edit`, `Write`, `sed` or scripts.
7. After editing: `pipeline_index.py FILE.ipynb --check` (JSON structure, syntax, local cells that use Spark = forgotten magic), then re-run the map.

## Notebook ↔ script pairs

A notebook is often the development copy of a production `.py` (comments like "Аналог process_row() из скрипта X", "порядок как в скрипте .py"). When changing a rule in one, find the counterpart and ask whether to change it too. Keep reference comments intact and say explicitly if your change breaks parity with the reference implementation.

## Databricks-exported .py

Files with `# COMMAND ----------` separators and `# MAGIC %sql` lines are notebooks saved as .py. The indexer splits them into cells but addresses lines as `L<file line>`; edit them with `Edit` like a normal .py file.
