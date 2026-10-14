#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline_index.py — статическая карта PySpark-скрипта или ноутбука.

Поддерживает: .py (обычный скрипт или экспорт Databricks), .ipynb (Jupyter / sparkmagic / Livy).
Только стандартная библиотека. Код НЕ исполняется — только разбор через ast/regex.

Использование:
  python3 pipeline_index.py FILE                  карта пайплайна (по умолчанию)
  python3 pipeline_index.py FILE --full           карта без сокращения цепочек трансформаций
  python3 pipeline_index.py FILE --df NAME        все присваивания и использования DataFrame NAME
  python3 pipeline_index.py FILE --col NAME       где колонка NAME создаётся, меняется, переименовывается, используется
  python3 pipeline_index.py FILE --emit DIR       для .ipynb: DIR/<имя>.cells.py (код ячеек без outputs)
                                                  и DIR/<имя>.outputs.txt (полезные outputs без виджетов)
  python3 pipeline_index.py FILE --check          статическая проверка после правки: синтаксис, структура .ipynb,
                                                  ячейки без %%spark, использующие Spark

Адресация: в .py — L<строка файла>; в .ipynb — c<номер ячейки>:<строка в ячейке> (нумерация с 0, как в индексе cells).
"""

import argparse
import ast
import io
import json
import re
import sys
import tokenize
from collections import Counter, OrderedDict, defaultdict
from pathlib import Path

# --------------------------------------------------------------------------- constants

REMOTE_MAGICS = {"spark", "pyspark"}
SQL_MAGICS = {"sql", "sparksql"}
SESSION_MAGICS = {"info", "logs", "cleanup", "delete", "send_to_spark", "help", "matplot", "manage_spark"}
PIPELINE_KINDS = {"PY", "REMOTE", "SQL"}

DF_METHODS = {
    "withColumn", "withColumns", "withColumnRenamed", "withColumnsRenamed", "where", "filter", "join",
    "crossJoin", "select", "selectExpr", "groupBy", "groupby", "agg", "union", "unionAll", "unionByName",
    "drop", "dropDuplicates", "drop_duplicates", "distinct", "fillna", "dropna", "orderBy", "sort",
    "sortWithinPartitions", "repartition", "repartitionByRange", "alias", "limit", "cache", "persist",
    "checkpoint", "localCheckpoint", "toDF", "hint", "transform", "unpivot", "melt", "sample",
    "exceptAll", "subtract", "intersect", "intersectAll", "replace", "na", "history",
}
TERMINAL_NON_DF = {
    "columns", "schema", "dtypes", "collect", "count", "first", "take", "head", "tail", "toPandas",
    "toLocalIterator", "show", "rdd", "isEmpty", "printSchema", "explain", "write", "writeTo",
    "createOrReplaceTempView", "createTempView", "toJSON", "foreach", "foreachPartition", "unpersist",
}
NON_DF_ROOTS = {"F", "functions", "Window", "W", "T", "types", "sf", "fn", "col", "lit", "when",
                "dt", "datetime", "pd", "np", "json", "os", "re", "time", "math", "pytz", "conf"}
DELTA_MUT = {"delete", "update", "merge", "vacuum", "restoreToVersion", "restoreToTimestamp", "optimize"}
PATH_IO = {"save", "parquet", "csv", "json", "orc", "text", "load"}
COLUMN_CALLS = {
    "col", "column", "expr", "alias", "withColumn", "withColumns", "withColumnRenamed", "select",
    "drop", "groupBy", "groupby", "orderBy", "sort", "partitionBy", "dropDuplicates", "drop_duplicates",
    "agg", "isin", "contains", "getField", "getItem", "selectExpr",
}
API_FLOOR = [("withColumns", "3.3"), ("withColumnsRenamed", "3.4"), ("unpivot", "3.4"),
             ("melt", "3.4"), ("allowMissingColumns", "3.1")]

TABLE_RE = re.compile(r"^[A-Za-z_]\w*\.[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?$")
FILE_EXT_RE = re.compile(r"\.(json|csv|py|txt|parquet|xlsx?|ipynb|sql|ya?ml|md|log)$", re.I)
WIDGET_RE = re.compile(r"^\s*(FloatProgress|Tab|MagicsControllerWidget|VBox|HBox|HTML|Output|IntProgress|Button)\(")
SQL_IN_RE = re.compile(r"\b(FROM|JOIN)\s+([A-Za-z_`][\w`]*(?:\.[A-Za-z_`][\w`]*){0,2})", re.I)
SQL_OUT_RE = re.compile(
    r"\b(INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|MERGE\s+INTO|CREATE\s+(?:OR\s+REPLACE\s+)?"
    r"(?:TEMP(?:ORARY)?\s+)?(?:TABLE|VIEW)(?:\s+IF\s+NOT\s+EXISTS)?|DELETE\s+FROM|UPDATE|TRUNCATE\s+TABLE|"
    r"DROP\s+TABLE(?:\s+IF\s+EXISTS)?)\s+([A-Za-z_`][\w`.]*)", re.I)

SECTION_RE = re.compile(r"^\s*(?:#\s*#{1,}|#{2,})\s*([^#\s].*)$")
STEP_PRINT_RE = re.compile(r"(Шаг|Step|STEP|>>>|===|\*{5})")
AUTHOR_RE = re.compile(r"--\s*([A-Za-z][\w.-]{2,})\s*$")
MARKER_RE = re.compile(r"(?i)^\s*#+\s*(update|доработк\w*|fix\b|изменени\w*|new\b|added|добавлен\w*)")
DATE_RE = re.compile(r"20\d\d-\d\d-\d\d")
TODO_RE = re.compile(r"(?i)\b(TODO|FIXME|XXX|HACK)\b|не\s+забыть|на\s+прод|для\s+прод|раскомм|расскомм|закомм")
COMMENTED_EXIT_RE = re.compile(r"^\s*#\s*(sys\.)?(exit|quit)\s*\(")
COMMENTED_DATE_RE = re.compile(r"^\s*#\s*\w+\s*=\s*[\w.]*(date|datetime)\(\s*\d{4}\s*,")
REF_RE = re.compile(r"(?i)(аналог\w*|по\s+аналогии|как\s+в\s|из\s+скрипта|логика\s+power|power\s+query|"
                    r"as\s+in\s|port(?:ed)?\s+(?:of|from)|same\s+as)")
SECRET_NAME_RE = re.compile(r"(?i)(pass(word)?|secret|token|api_?key|access_?key|login)")
CYR_RE = re.compile(r"[А-Яа-яЁё]")


def short(node, n=30):
    try:
        s = ast.unparse(node)
    except Exception:
        s = "?"
    s = re.sub(r"\s+", " ", s)
    return s if len(s) <= n else s[: n - 1] + "…"


def clip(s, n):
    s = re.sub(r"\s+", " ", s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def chain(node):
    """Разворачивает цепочку a.b(...).c(...) → (root, [(method, call_or_None), ...]) в порядке вызова."""
    ops = []
    while True:
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            ops.append((node.func.attr, node))
            node = node.func.value
        elif isinstance(node, ast.Attribute):
            ops.append((node.attr, None))
            node = node.value
        elif isinstance(node, ast.Subscript):
            node = node.value
        else:
            break
    ops.reverse()
    return node, ops


def is_exit_call(n):
    return isinstance(n, ast.Call) and (
        (isinstance(n.func, ast.Name) and n.func.id in ("exit", "quit"))
        or (isinstance(n.func, ast.Attribute) and n.func.attr == "exit"
            and isinstance(n.func.value, ast.Name) and n.func.value.id in ("sys", "os")))


def is_barrier(tree):
    body = [s for s in tree.body if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))]
    if not body:
        return False
    for s in body:
        if isinstance(s, ast.Raise):
            continue
        if isinstance(s, ast.Assert) and isinstance(s.test, ast.Constant) and not s.test.value:
            continue
        if isinstance(s, ast.Expr) and is_exit_call(s.value):
            continue
        return False
    return True


def norm_table(name, catalogs=None):
    name = name.replace("`", "")
    parts = name.split(".")
    if len(parts) == 3:
        if catalogs is not None:
            catalogs.add(parts[0])
        return ".".join(parts[1:])
    return name


# --------------------------------------------------------------------------- units

class Unit:
    """Ячейка ноутбука или весь .py-файл (offset != None → адресация строками файла)."""

    def __init__(self, idx, cid, kind, exec_count, src, offset=None, magic=None):
        self.idx, self.cid, self.kind, self.exec = idx, cid, kind, exec_count
        self.src, self.offset, self.magic = src, offset, magic
        self.lines = src.splitlines()
        self.tree = None
        self.err = None
        self.barrier = False
        self.after_barrier = False
        self.outputs = []
        self.vrange = None

    def loc(self, lineno):
        if self.offset is not None:
            return f"L{self.offset + lineno}"
        return f"c{self.idx}:{lineno}"

    def pos(self, lineno):
        return ((self.idx or 0), (self.offset or 0) + lineno)

    def text(self, lineno):
        return self.lines[lineno - 1].strip() if 0 < lineno <= len(self.lines) else ""

    def pycode(self):
        out = []
        for line in self.lines:
            s = line.lstrip()
            out.append(("#" + line) if s.startswith(("%", "!")) else line)
        return "\n".join(out)

    def parse(self):
        try:
            self.tree = ast.parse(self.pycode())
        except SyntaxError as e:
            self.err = f"{self.loc(e.lineno or 1)}: {e.msg}"


def classify_cell(src, default_kind):
    lines = src.splitlines()
    first = next((l.strip() for l in lines if l.strip()), "")
    if not first:
        return "EMPTY", None
    m = re.match(r"%%(\w+)(.*)", first)
    if m:
        mg, args = m.group(1).lower(), m.group(2)
        if mg in REMOTE_MAGICS:
            return ("SQL" if re.search(r"-c\s+sql", args) else "REMOTE"), first
        if mg in SQL_MAGICS:
            return "SQL", first
        if mg == "local":
            return "LOCAL", first
        if mg == "configure":
            return "CONFIG", first
        if mg in SESSION_MAGICS:
            return "SESSION", first
        return default_kind, first
    if re.match(r"#\s*%%\s*(spark|pyspark|sql|sparksql|local|configure)\b", first):
        return "DISABLED", first
    if re.match(r"%run\b", first):
        return "RUN", first
    code = [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
    if code and all(l.startswith(("%", "!")) for l in code):
        return "SESSION", None
    return default_kind, None


def load_ipynb(path):
    nb = json.loads(Path(path).read_text(encoding="utf-8"))
    ks = nb.get("metadata", {}).get("kernelspec") or {}
    kname = f"{ks.get('name', '')} {ks.get('display_name', '')}".strip()
    cells = nb.get("cells", [])

    def src_of(c):
        s = c.get("source", "")
        return "".join(s) if isinstance(s, list) else s

    srcs = [src_of(c) for c in cells]
    sparkmagic = any(re.search(r"^\s*%%(spark|sql)\b|sparkmagic", s, re.M) for s in srcs)
    spark_kernel = bool(re.search(r"pyspark|sparkkernel|sparkmagic", kname, re.I))
    default_kind = "REMOTE" if spark_kernel else ("LOCAL" if sparkmagic else "PY")

    units = []
    for i, (c, s) in enumerate(zip(cells, srcs)):
        ctype = c.get("cell_type")
        if ctype == "markdown":
            u = Unit(i, c.get("id"), "MD", None, s)
        elif ctype == "code":
            kind, magic = classify_cell(s, default_kind)
            u = Unit(i, c.get("id"), kind, c.get("execution_count"), s, magic=magic)
            u.outputs = c.get("outputs", [])
        else:
            u = Unit(i, c.get("id"), "RAW", None, s)
        units.append(u)
    meta = {"kernel": kname or "?", "sparkmagic": sparkmagic, "spark_kernel": spark_kernel,
            "default_kind": default_kind, "nbformat": f"{nb.get('nbformat')}.{nb.get('nbformat_minor')}"}
    return units, meta, nb


def load_py(path):
    text = Path(path).read_text(encoding="utf-8")
    if not re.search(r"^# COMMAND -{5,}\s*$", text, re.M):
        return [Unit(None, None, "PY", None, text, offset=0)], {"databricks": False}
    units, offset = [], 0
    parts = re.split(r"(^# COMMAND -{5,}\s*\n)", text, flags=re.M)
    idx = 0
    for p in parts:
        if re.match(r"^# COMMAND -{5,}", p):
            offset += p.count("\n")
            continue
        lines = ["" if l.startswith("# Databricks notebook source") else l for l in p.splitlines()]
        magic_lines = [l for l in lines if l.strip()]
        if magic_lines and all(l.startswith("# MAGIC") for l in magic_lines):
            body = "\n".join(re.sub(r"^# MAGIC ?", "", l) for l in lines)
            first = next((l.strip() for l in body.splitlines() if l.strip()), "")
            kind = ("MD" if first.startswith("%md") else "SQL" if first.startswith("%sql")
                    else "RUN" if first.startswith("%run") else "PY")
            u = Unit(idx, None, kind, None, body, offset=offset, magic=first)
        else:
            u = Unit(idx, None, "PY", None, "\n".join(lines), offset=offset)
        units.append(u)
        offset += p.count("\n")
        idx += 1
    return units, {"databricks": True}


# --------------------------------------------------------------------------- analyzer

class Analyzer:
    def __init__(self, units, want_col=None, want_df=None):
        self.units = units
        self.want_col = want_col
        self.want_df = want_df
        self.known = set()
        self.consts = {}
        self.delta_vars = {}
        self.catalogs = set()
        self.flow = []                      # (pos, kind, text)
        self.df_assigns = []                # dict
        self.df_uses = defaultdict(list)
        self.tables = OrderedDict()         # name -> {"in": [], "out": []}
        self.col_events = []
        self.clones = defaultdict(list)
        self.literals = defaultdict(OrderedDict)
        self.risks = defaultdict(list)
        self.guards = []
        self.session = []
        self.params = []
        self.funcs = []
        self.markers = []
        self.authors = Counter()
        self.refs = []
        self.style = Counter()
        self.api = OrderedDict()
        self.mentioned = defaultdict(list)
        self.has_unpersist = False

    # ---- helpers
    def sval(self, node):
        if node is None:
            return None
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            out = []
            for v in node.values:
                if isinstance(v, ast.Constant):
                    out.append(str(v.value))
                elif isinstance(v, ast.FormattedValue):
                    inner = self.sval(v.value) if isinstance(v.value, ast.Name) else None
                    out.append(inner if inner is not None else "{" + short(v.value, 25) + "}")
            return "".join(out)
        if isinstance(node, ast.Name):
            return self.consts.get(node.id)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            a, b = self.sval(node.left), self.sval(node.right)
            return a + b if a is not None and b is not None else None
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format"
                and isinstance(node.func.value, ast.Constant) and isinstance(node.func.value.value, str)):
            return node.func.value.value
        return None

    def sdisp(self, node):
        v = self.sval(node)
        if v is not None:
            return v
        return f"<{short(node, 25)}>" if node is not None else "?"

    def add_table(self, name, direction, loc, unit, op=""):
        if not name:
            return
        name = norm_table(name, self.catalogs) if TABLE_RE.match(name.replace("`", "")) else name
        rec = self.tables.setdefault(name, {"in": [], "out": []})
        tag = loc + (f" {op}" if op else "") + (" ⚠после-барьера" if unit.after_barrier else "")
        rec[direction].append(tag)

    def root_desc(self, root, ops):
        if isinstance(root, ast.Name):
            if root.id == "spark" and ops:
                m, c = ops[0]
                if m == "table" and c is not None and c.args:
                    return f"spark.table({self.sdisp(c.args[0])})"
                if m == "sql" and c is not None and c.args:
                    sql = self.sval(c.args[0]) or ""
                    ins = sorted({norm_table(t) for t in self.sql_tables(sql)[0]})
                    return f"spark.sql(FROM {', '.join(ins)})" if ins else f"spark.sql({clip(repr(sql), 50)})"
                if m == "read":
                    for mm, cc in ops:
                        if mm in PATH_IO | {"table"} and cc is not None and cc.args:
                            return f"spark.read.{mm}({self.sdisp(cc.args[0])})"
                    return "spark.read"
                return f"spark.{m}"
            return root.id
        if isinstance(root, ast.Call):
            fname = short(root.func, 40)
            args = [self.sval(a) or (a.id if isinstance(a, ast.Name) else "…") for a in root.args]
            args += [self.sval(k.value) or (k.value.id if isinstance(k.value, ast.Name) else "…")
                     for k in root.keywords]
            args = [a for a in args if a not in ("spark",)]
            return f"{fname}({', '.join(clip(a, 40) for a in args)})"
        return short(root, 40)

    def summarize(self, ops, full=False, skip_source=False):
        out, where_n = [], 0
        for m, c in ops:
            if c is None:
                continue
            a = c.args
            kw = {k.arg: k.value for k in c.keywords}
            if m in ("where", "filter"):
                where_n += 1
                continue
            if where_n:
                out.append(f"where×{where_n}" if where_n > 1 else "where")
                where_n = 0
            if skip_source and m in ("table", "sql", "read", "format", "load", "option", "options", "parquet", "csv"):
                continue
            if m == "withColumn" and a:
                tag = "+" + (self.sval(a[0]) or "?")
                if len(a) > 1:
                    tag += self.expr_tags(a[1])
                out.append(tag)
            elif m == "withColumns" and a and isinstance(a[0], ast.Dict):
                for k, v in zip(a[0].keys, a[0].values):
                    out.append("+" + (self.sval(k) or "?") + self.expr_tags(v))
            elif m == "withColumnRenamed" and len(a) >= 2:
                out.append(f"{self.sval(a[0])}→{self.sval(a[1])}")
            elif m == "withColumnsRenamed" and a and isinstance(a[0], ast.Dict):
                for k, v in zip(a[0].keys, a[0].values):
                    out.append(f"{self.sval(k)}→{self.sval(v)}")
            elif m in ("select", "selectExpr"):
                n = sum(len(x.elts) if isinstance(x, (ast.List, ast.Tuple)) else 1 for x in a)
                out.append(f"select({n})")
            elif m == "drop":
                out.append("-" + ",".join(self.sval(x) or "…" for x in a))
            elif m == "join":
                other = short(a[0], 28) if a else "?"
                how = self.sval(kw.get("how")) or (self.sval(a[2]) if len(a) > 2 else None) or "inner"
                out.append(f"join({other},{how})")
            elif m == "crossJoin":
                out.append(f"crossJoin({short(a[0], 28) if a else '?'})")
            elif m in ("groupBy", "groupby"):
                out.append(f"groupBy({', '.join(short(x, 20) for x in a)})")
            elif m == "agg":
                out.append("agg")
            elif m in ("union", "unionAll", "unionByName"):
                miss = any(k.arg == "allowMissingColumns" for k in c.keywords)
                pref = "∪" if m == "unionByName" else "∪[по позиции]"
                out.append(pref + (short(a[0], 28) if a else "?") + ("(+missing)" if miss else ""))
            elif m in ("dropDuplicates", "drop_duplicates"):
                out.append("dedup" + (f"({short(a[0], 30)})" if a else ""))
            elif m in ("distinct", "fillna", "dropna", "cache", "persist", "checkpoint", "localCheckpoint", "limit"):
                out.append(m)
            elif m in ("orderBy", "sort"):
                out.append("orderBy")
            elif m in ("repartition", "coalesce", "repartitionByRange"):
                out.append(f"{m}({', '.join(short(x, 15) for x in a)})")
            elif m == "alias" and a:
                out.append(f"alias({self.sval(a[0])})")
            elif m in DF_METHODS:
                out.append(m)
        if where_n:
            out.append(f"where×{where_n}" if where_n > 1 else "where")
        if not full:
            out = self.compress_cols(out)
        return " · ".join(out)

    @staticmethod
    def compress_cols(tokens, keep=3):
        res, run = [], []

        def flush():
            if len(run) > keep + 1:
                res.append(" ".join(run[:keep]) + f" …(+{len(run) - keep} кол.)")
            elif run:
                res.append(" ".join(run))
            run.clear()

        for t in tokens:
            if t.startswith("+"):
                run.append(t)
            else:
                flush()
                res.append(t)
        flush()
        return res

    @staticmethod
    def expr_tags(expr):
        tags = []
        for n in ast.walk(expr):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
                if n.func.attr == "over":
                    tags.append("win")
                elif n.func.attr in ("explode", "explode_outer", "posexplode"):
                    tags.append("explode")
        return f"[{','.join(sorted(set(tags)))}]" if tags else ""

    def sql_tables(self, sql):
        ins, outs = [], []
        for m in SQL_OUT_RE.finditer(sql or ""):
            outs.append((m.group(2), clip(m.group(1), 20)))
        for m in SQL_IN_RE.finditer(sql or ""):
            pre = sql[: m.start()].rstrip().upper()
            name = m.group(2)
            if pre.endswith("DELETE"):
                continue
            if "." in name or name in self.known_views():
                ins.append(name)
        return ins, outs

    def known_views(self):
        return {t[5:] for t in self.tables if t.startswith("view:")}

    # ---- main walk
    def iter_stmts(self, body, ctx):
        for st in body:
            if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield "def", st, list(st.decorator_list), ctx
                yield from self.iter_stmts(st.body, dict(ctx, func=st.name))
            elif isinstance(st, ast.ClassDef):
                yield from self.iter_stmts(st.body, ctx)
            elif isinstance(st, (ast.For, ast.AsyncFor)):
                yield "loop", st, [st.iter], ctx
                yield from self.iter_stmts(st.body, dict(ctx, loop=ctx.get("loop", 0) + 1))
                yield from self.iter_stmts(st.orelse, ctx)
            elif isinstance(st, ast.While):
                yield "loop", st, [st.test], ctx
                yield from self.iter_stmts(st.body, dict(ctx, loop=ctx.get("loop", 0) + 1))
                yield from self.iter_stmts(st.orelse, ctx)
            elif isinstance(st, ast.If):
                yield "if", st, [st.test], ctx
                yield from self.iter_stmts(st.body, ctx)
                yield from self.iter_stmts(st.orelse, ctx)
            elif isinstance(st, (ast.With, ast.AsyncWith)):
                yield "with", st, [i.context_expr for i in st.items], ctx
                yield from self.iter_stmts(st.body, ctx)
            elif isinstance(st, ast.Try) or type(st).__name__ == "TryStar":
                yield from self.iter_stmts(st.body, ctx)
                for h in st.handlers:
                    yield from self.iter_stmts(h.body, ctx)
                yield from self.iter_stmts(st.orelse, ctx)
                yield from self.iter_stmts(st.finalbody, ctx)
            else:
                yield "simple", st, [st], ctx

    def run(self):
        barrier_seen = False
        for u in self.units:
            if u.kind in ("PY", "REMOTE", "LOCAL"):
                u.parse()
                if u.tree is not None and u.offset is None and is_barrier(u.tree) and not barrier_seen:
                    u.barrier = True
                    barrier_seen = True
                    self.flow.append((u.pos(1), "barrier", f"── БАРЬЕР {u.loc(1)}: {clip(u.text(1), 40)} — ниже ручные ячейки ──"))
                    continue
            if barrier_seen and not u.barrier:
                u.after_barrier = True

        for u in self.units:
            self.scan_comments(u)
            if u.kind == "MD":
                for ln, line in enumerate(u.lines, 1):
                    if re.match(r"^\s*#{1,6}\s+\S", line):
                        self.flow.append((u.pos(ln), "section", f"§ {u.loc(ln)} {clip(line.strip('# '), 80)}"))
                    if REF_RE.search(line):
                        self.refs.append(f"{u.loc(ln)} {clip(line, 110)}")
                continue
            if u.kind == "SQL":
                body = "\n".join(l for l in u.lines if not l.strip().startswith("%"))
                self.handle_sql(body, u, u.loc(1))
                continue
            if u.kind == "CONFIG":
                self.session.append(f"{u.loc(1)} %%configure {clip(' '.join(u.lines[1:]), 160)}")
                continue
            if u.kind == "RUN":
                self.flow.append((u.pos(1), "run", f"⇢ {u.loc(1)} {clip(u.lines[0] if u.lines else '', 80)}  (подключает чужой ноутбук)"))
                continue
            if u.tree is None or u.barrier:
                continue
            if u.kind == "LOCAL":
                continue  # локальные ячейки в sparkmagic-ноутбуке: только для --check/предупреждений
            self.walk_unit(u)

    def scan_comments(self, u):
        if u.kind in ("MD", "RAW", "EMPTY"):
            return
        for ln, line in enumerate(u.lines, 1):
            s = line.strip()
            if not s.startswith("#") and "#" not in s:
                continue
            comment = s if s.startswith("#") else s[s.index("#"):]
            if s.startswith("#"):
                m = SECTION_RE.match(s)
                if m and u.kind in PIPELINE_KINDS and not u.after_barrier:
                    self.flow.append((u.pos(ln), "section", f"§ {u.loc(ln)} {clip(m.group(1), 80)}"))
                if COMMENTED_EXIT_RE.match(s):
                    self.guards.append(f"{u.loc(ln)} закомментированный выход (переключатель отладки): {clip(s, 60)}")
                if COMMENTED_DATE_RE.match(s):
                    self.guards.append(f"{u.loc(ln)} закомментированная фиксированная дата (режим пересчёта): {clip(s, 70)}")
            a = AUTHOR_RE.search(comment)
            if a:
                self.authors[a.group(1)] += 1
            if MARKER_RE.match(comment) or (DATE_RE.search(comment) and comment.startswith("#")):
                self.markers.append(f"{u.loc(ln)} {clip(comment, 100)}")
            if TODO_RE.search(comment):
                self.guards.append(f"{u.loc(ln)} {clip(comment, 100)}")
            if REF_RE.search(comment):
                self.refs.append(f"{u.loc(ln)} {clip(comment, 110)}")
            if CYR_RE.search(comment):
                self.style["comment_ru"] += 1
            elif re.search(r"[A-Za-z]{3,}", comment):
                self.style["comment_en"] += 1

    def walk_unit(self, u):
        for kind, st, scan, ctx in self.iter_stmts(u.tree.body, {}):
            lineno = getattr(st, "lineno", 1)
            loc = u.loc(lineno)
            if kind == "def":
                doc = ast.get_docstring(st) or ""
                params = [a.arg for a in st.args.args]
                self.funcs.append(f"{loc} {st.name}({', '.join(params)})" + (f" — {clip(doc.splitlines()[0], 70)}" if doc else ""))
                for a in st.args.args:
                    ann = short(a.annotation, 30) if a.annotation is not None else ""
                    if a.arg.startswith("df") or "DataFrame" in ann:
                        self.known.add(a.arg)
                for d in st.decorator_list:
                    if "udf" in short(d, 40):
                        self.risks["UDF"].append(f"{loc} @{short(d, 30)} {st.name}")
                continue

            target = None
            if isinstance(st, ast.Assign) and len(st.targets) == 1 and isinstance(st.targets[0], ast.Name):
                target = st.targets[0].id
            elif isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name):
                target = st.target.id

            # использования известных DataFrame
            if self.want_df:
                for n in ast.walk(st if kind == "simple" else scan[0]):
                    if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load) and n.id == self.want_df:
                        self.df_uses[n.id].append((u.pos(lineno), loc, target, u.text(lineno)))
                        break

            for root_node in scan:
                for n in ast.walk(root_node):
                    if isinstance(n, ast.Call):
                        self.handle_call(n, u, ctx, target)
                    elif self.want_col and isinstance(n, ast.Attribute) and n.attr == self.want_col \
                            and isinstance(n.value, ast.Name) and n.value.id in self.known:
                        self.col_event("REF", u, n.lineno, target, f"{n.value.id}.{n.attr}")
                    elif self.want_col and isinstance(n, ast.Subscript) and isinstance(n.value, ast.Name) \
                            and n.value.id in self.known and self.sval(n.slice) == self.want_col:
                        self.col_event("REF", u, n.lineno, target, f"{n.value.id}[...]")

            if kind == "simple" and isinstance(st, ast.Expr) and is_exit_call(st.value):
                self.guards.append(f"{loc} АКТИВНЫЙ {short(st.value, 20)} — код ниже не выполняется")
                self.flow.append((u.pos(lineno), "barrier", f"── {loc} АКТИВНЫЙ {short(st.value, 20)} ──"))

            if kind == "simple" and isinstance(st, ast.Expr) and isinstance(st.value, ast.Call) \
                    and isinstance(st.value.func, ast.Name) and st.value.func.id == "print" and st.value.args:
                s = self.sval(st.value.args[0]) or ""
                if STEP_PRINT_RE.search(s):
                    cleaned = re.sub(r"[>*=#]{3,}", " ", s).strip(" *")
                    if len(cleaned) > 3 and u.kind in PIPELINE_KINDS:
                        self.flow.append((u.pos(lineno), "section", f"§ {loc} print: {clip(cleaned, 80)}"))

            if target is None or kind != "simple":
                continue
            value = st.value
            if value is None:
                continue
            if self.is_df_value(value):
                root, ops = chain(value)
                is_spark_src = isinstance(root, ast.Name) and root.id == "spark"
                rec = {
                    "name": target, "loc": loc, "pos": u.pos(lineno), "unit": u,
                    "root": self.root_desc(root, ops), "ops": ops, "skip_source": is_spark_src,
                    "loop": ctx.get("loop", 0) > 0, "func": ctx.get("func"),
                }
                self.df_assigns.append(rec)
                if rec["loop"] and target in self.known:
                    self.risks["LOOP_DF"].append(f"{loc} {target} переприсваивается в цикле — план растёт с каждой итерацией")
                self.known.add(target)
                continue
            if isinstance(value, (ast.List, ast.Tuple)):
                for el in value.elts:
                    if isinstance(el, (ast.Tuple, ast.List)):
                        dfs = [e.id for e in el.elts if isinstance(e, ast.Name) and e.id in self.known]
                        tbls = [self.sval(e) for e in el.elts if isinstance(e, ast.Constant)]
                        tbls = [t for t in tbls if t and TABLE_RE.match(t)]
                        if dfs and tbls:
                            self.add_table(tbls[0], "out", u.loc(el.lineno), u, op=f"? список {target}: {dfs[0]} (запись в цикле)")
            sv = self.sval(value)
            if sv is not None and isinstance(value, (ast.Constant, ast.JoinedStr, ast.BinOp)):
                self.consts[target] = sv
                if re.match(r"^[A-Z][A-Z0-9_]+$", target) and ctx.get("func") in (None, "main"):
                    self.params.append(f"{loc} {target} = {clip(repr(sv), 70)}")
                if SECRET_NAME_RE.search(target) and sv:
                    self.guards.append(f"{loc} строковый литерал в {target} — похоже на учётные данные (значение скрыто)")
                continue
            root, ops = chain(value)
            names = [m for m, _ in ops]
            if isinstance(root, ast.Name) and root.id == "DeltaTable" and names[:1] and names[0] in ("forName", "forPath"):
                c = ops[0][1]
                if c is not None and len(c.args) >= 2:
                    self.delta_vars[target] = self.sdisp(c.args[1])
                continue
            if ctx.get("func") in (None, "main"):
                src = short(value, 200)
                if re.search(r"\b(today|now|utcnow)\(", src):
                    self.params.append(f"{loc} {target} = {clip(src, 70)}  (дата запуска)")
                elif re.match(r"^[A-Z][A-Z0-9_]+$", target) and isinstance(value, (ast.Constant, ast.Call, ast.BinOp, ast.UnaryOp, ast.List, ast.Tuple)):
                    self.params.append(f"{loc} {target} = {clip(src, 70)}")
                if isinstance(value, ast.Call) and re.search(r"(date|datetime)\(\s*20\d\d\s*,", src) and "today" not in src:
                    self.guards.append(f"{loc} АКТИВНАЯ фиксированная дата: {target} = {clip(src, 50)}")

    def is_df_value(self, v):
        root, ops = chain(v)
        names = [m for m, _ in ops]
        if any(n in TERMINAL_NON_DF for n in names):
            return False
        rid = root.id if isinstance(root, ast.Name) else None
        if rid in NON_DF_ROOTS:
            return False
        if rid == "DeltaTable":
            return "toDF" in names or "history" in names
        if rid in self.known:
            return True
        if rid == "spark":
            return bool(names) and names[0] in ("table", "sql", "read", "createDataFrame", "range")
        if isinstance(root, ast.Call):
            if self.generic_io(root, record=False)[0] == "in":
                return True
            args = list(root.args) + [k.value for k in root.keywords]
            if any(isinstance(a, ast.Name) and a.id in self.known for a in args):
                return True
            return any(n in DF_METHODS for n in names)
        return rid is not None and any(n in DF_METHODS for n in names)

    def generic_io(self, call, record=True, u=None, loc=None):
        """Вызов функции-обёртки, у которой в аргументах имя таблицы (read_table_retry(spark, 'a.b') и т.п.)."""
        f = call.func
        fname = f.id if isinstance(f, ast.Name) else (f.attr if isinstance(f, ast.Attribute) else "")
        low = fname.lower()
        direction = None
        if re.search(r"write|save|overwrite|insert|append|merge|upsert", low):
            direction = "out"
        elif re.search(r"read|load|table|get_df", low):
            direction = "in"
        if not direction:
            return None, None
        for a in list(call.args) + [k.value for k in call.keywords]:
            s = self.sval(a)
            if s and TABLE_RE.match(s) and not FILE_EXT_RE.search(s):
                if record:
                    self.add_table(s, direction, loc, u, op=f"via {fname}()")
                return direction, s
        return None, None

    def handle_sql(self, sql, u, loc):
        ins, outs = self.sql_tables(sql)
        views = self.known_views()
        for t in ins:
            self.add_table("view:" + t if t in views else t, "in", loc, u, op="sql")
        for t, op in outs:
            self.add_table(t, "out", loc, u, op=f"sql {op}")
            self.flow.append((u.pos(1), "write", f"⇒ {loc} SQL {op.upper()} {norm_table(t)}"))
        if self.want_col and re.search(rf"\b{re.escape(self.want_col)}\b", sql or ""):
            line = next((l for l in sql.splitlines() if re.search(rf"\b{re.escape(self.want_col)}\b", l)), "")
            self.col_events.append((u.pos(1), loc, "SQL", None, clip(line, 120)))

    def col_event(self, kind, u, lineno, target, detail=""):
        self.col_events.append((u.pos(lineno), u.loc(lineno), kind, target, clip(detail or u.text(lineno), 120)))

    def handle_call(self, c, u, ctx, target):
        f = c.func
        # строка самого метода в многострочной цепочке, а не начала выражения
        ml = f.end_lineno if isinstance(f, ast.Attribute) and f.end_lineno else c.lineno
        loc = u.loc(ml)
        if not isinstance(f, ast.Attribute):
            if isinstance(f, ast.Name):
                if f.id in ("udf", "pandas_udf"):
                    self.risks["UDF"].append(f"{loc} {f.id}(...)")
                self.generic_io(c, record=True, u=u, loc=loc)
                if self.want_col and f.id in ("col", "column") and c.args:
                    s = self.sval(c.args[0])
                    if s and (s == self.want_col or s.endswith("." + self.want_col)):
                        self.col_event("REF", u, ml, target)
            return
        m = f.attr
        root, ops = chain(c)
        names = [x for x, _ in ops]
        rid = root.id if isinstance(root, ast.Name) else None
        a = c.args
        kw = {k.arg: k.value for k in c.keywords}

        # ---- API floor / style
        for api, ver in API_FLOOR:
            if m == api or api in kw:
                self.api.setdefault(api, (ver, loc))
        if m == "unpersist":
            self.has_unpersist = True

        # ---- tables
        if m == "table" and (rid == "spark" and names[0] == "table" or "read" in names[:-1]) and a:
            self.add_table(self.sdisp(a[0]), "in", loc, u)
        elif m in ("saveAsTable", "insertInto") and (a or "name" in kw):
            mode = self.sval(kw.get("mode"))
            extra = []
            for mm, cc in ops:
                if cc is None:
                    continue
                if mm == "mode" and cc.args:
                    mode = self.sval(cc.args[0])
                elif mm == "partitionBy":
                    extra.append("partitionBy(" + ",".join(self.sval(x) or "…" for x in cc.args) + ")")
                elif mm == "option" and len(cc.args) >= 2 and self.sval(cc.args[0]) in ("overwriteSchema", "mergeSchema", "replaceWhere"):
                    extra.append(f"{self.sval(cc.args[0])}={self.sval(cc.args[1])}")
            op = " ".join(x for x in [m, mode or "", *extra] if x)
            tname = self.sdisp(a[0] if a else kw.get("name"))
            if tname.startswith("<") and ctx.get("func"):
                tname += f" — параметр def {ctx['func']}, реальные имена смотри у вызовов"
            self.add_table(tname, "out", loc, u, op=op)
            self.flow.append((u.pos(ml), "write", f"⇒ {loc} WRITE {tname} [{op}]"))
        elif m in PATH_IO and ("write" in names or "read" in names) and (a or "path" in kw):
            p = self.sdisp(a[0] if a else kw.get("path"))
            if "write" in names:
                mode = next((self.sval(cc.args[0]) for mm, cc in ops if mm == "mode" and cc is not None and cc.args), None) or self.sval(kw.get("mode"))
                self.add_table("path:" + p, "out", loc, u, op=f"{m} {mode or ''}".strip())
                self.flow.append((u.pos(ml), "write", f"⇒ {loc} WRITE path:{clip(p, 60)} [{m} {mode or ''}]"))
            else:
                self.add_table("path:" + p, "in", loc, u, op=m)
        elif m in DELTA_MUT:
            tbl = None
            if rid in self.delta_vars:
                tbl = self.delta_vars[rid]
            else:
                for mm, cc in ops:
                    if mm in ("forName", "forPath") and cc is not None and len(cc.args) >= 2:
                        tbl = self.sdisp(cc.args[1])
            if tbl:
                self.add_table(tbl, "out", loc, u, op=f"DeltaTable.{m}")
                self.flow.append((u.pos(ml), "write", f"⇒ {loc} DELTA {m.upper()} {tbl}"))
        elif m == "sql" and rid == "spark" and a:
            self.handle_sql(self.sval(a[0]) or "", u, loc)
        elif m in ("createOrReplaceTempView", "createTempView", "createOrReplaceGlobalTempView", "createGlobalTempView") and a:
            self.add_table("view:" + self.sdisp(a[0]), "out", loc, u, op=m)
        elif m in ("set", "config") and len(a) >= 2 and ("conf" in names or rid in ("conf", "SparkConf") or "builder" in names):
            self.session.append(f"{loc} {self.sdisp(a[0])} = {clip(self.sdisp(a[1]), 80)}")
        elif m == "add_argument" and a:
            d = kw.get("default")
            self.params.append(f"{loc} argparse {self.sdisp(a[0])}" + (f" (default={short(d, 30)})" if d is not None else ""))
        elif m in ("get", "text", "dropdown") and "widgets" in names and a:
            self.params.append(f"{loc} widget {self.sdisp(a[0])}")
        elif rid not in ("spark", "F", "DeltaTable") and m not in DF_METHODS and m not in TERMINAL_NON_DF:
            self.generic_io(c, record=True, u=u, loc=loc)

        # строки вида 'schema.table', направление которых неизвестно
        if m not in COLUMN_CALLS and m not in ("set", "config", "get", "option", "options") and rid not in ("F",):
            for x in a:
                s = self.sval(x) if isinstance(x, ast.Constant) else None
                if s and TABLE_RE.match(s) and not FILE_EXT_RE.search(s) and s.split(".")[0] not in self.known \
                        and not s.startswith(("spark.", "hive.", "hadoop.", "parquet.", "delta.")):
                    self.mentioned[norm_table(s)].append(loc)

        # ---- clones / literals / column events
        if m == "withColumn" and len(a) >= 2:
            name = self.sval(a[0]) or "?"
            key = "withColumn: " + short(a[1], 10_000)
            if len(key) >= 60:
                self.clones[key].append((u.pos(ml), loc, name))
            self.collect_literals(name, a[1], loc)
            if self.want_col and name == self.want_col:
                self.col_event("SET", u, ml, target, f"withColumn = {short(a[1], 100)}")
        elif m == "withColumns" and a and isinstance(a[0], ast.Dict):
            for k, v in zip(a[0].keys, a[0].values):
                name = self.sval(k) or "?"
                key = "withColumn: " + short(v, 10_000)
                if len(key) >= 60:
                    self.clones[key].append((u.pos(ml), loc, name))
                self.collect_literals(name, v, loc)
                if self.want_col and name == self.want_col:
                    self.col_event("SET", u, v.lineno, target, f"withColumns = {short(v, 100)}")
        elif m in ("where", "filter") and a:
            key = "where: " + short(a[0], 10_000)
            if len(key) >= 50:
                self.clones[key].append((u.pos(ml), loc, ""))

        if self.want_col:
            wc = self.want_col
            if m == "withColumnRenamed" and len(a) >= 2:
                x, y = self.sval(a[0]), self.sval(a[1])
                if x == wc:
                    self.col_event("RENAME→", u, ml, target, f"{x} → {y}")
                if y == wc:
                    self.col_event("RENAME←", u, ml, target, f"{x} → {y}")
            elif m == "withColumnsRenamed" and a and isinstance(a[0], ast.Dict):
                for k, v in zip(a[0].keys, a[0].values):
                    if wc in (self.sval(k), self.sval(v)):
                        self.col_event("RENAME", u, ml, target, f"{self.sval(k)} → {self.sval(v)}")
            elif m == "alias" and a and self.sval(a[0]) == wc and rid not in self.known:
                self.col_event("ALIAS", u, ml, target, f"… .alias({wc}) ← {short(f.value, 90)}")
            elif m == "drop" and any(self.sval(x) == wc for x in a):
                self.col_event("DROP", u, ml, target)
            elif rid in ("F", "functions", "sf") and m in ("col", "column") and a:
                s = self.sval(a[0])
                if s and (s == wc or s.endswith("." + wc)):
                    self.col_event("REF", u, ml, target)
            elif rid in ("F", "functions", "sf") and any(self.sval(x) == wc for x in a):
                self.col_event("REF", u, ml, target, f"F.{m}({wc})")
            elif m in ("select", "groupBy", "groupby", "partitionBy", "orderBy", "sort", "dropDuplicates",
                       "drop_duplicates", "join", "agg", "fillna", "dropna"):
                vals = []
                for x in list(a) + list(kw.values()):
                    if isinstance(x, (ast.List, ast.Tuple)):
                        vals += [self.sval(e) for e in x.elts]
                    else:
                        vals.append(self.sval(x))
                if wc in vals:
                    self.col_event("REF", u, ml, target, f".{m}(… {wc} …)")

        # ---- risks
        if m in ("collect", "toPandas", "toLocalIterator"):
            self.risks["DRIVER"].append(f"{loc} .{m}() — {clip(u.text(ml), 70)}")
        elif m == "show":
            self.risks["DEBUG_SHOW"].append(loc)
        elif m == "orderBy" and rid == "Window" and "partitionBy" not in names:
            self.risks["WINDOW_NO_PARTITION"].append(f"{loc} {clip(u.text(ml), 70)}")
        elif m in ("udf", "pandas_udf"):
            self.risks["UDF"].append(f"{loc} {short(c, 50)}")
        elif m in ("dropDuplicates", "drop_duplicates") and a:
            self.risks["DEDUP_SUBSET"].append(f"{loc} dedup({short(a[0], 40)}) — какая строка остаётся, не определено")
        elif m == "isin" and any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "collect"
                                 for x in a for n in ast.walk(x)):
            self.risks["ISIN_COLLECT"].append(f"{loc} isin(<collect>) — данные идут через драйвер")
        elif m == "crossJoin" or (m == "join" and len(a) < 2 and "on" not in kw and (rid in self.known or any(n in DF_METHODS for n in names[:-1]))):
            self.risks["CROSS_JOIN"].append(f"{loc} {clip(u.text(ml), 70)}")
        elif m in ("cache", "persist"):
            self.risks["CACHE"].append(loc)
        elif m in ("coalesce", "repartition") and rid not in ("F", "functions") and len(a) == 1 \
                and isinstance(a[0], ast.Constant) and a[0].value == 1:
            self.risks["SINGLE_PARTITION"].append(f"{loc} .{m}(1)")
        elif m in ("union", "unionAll") and (rid in self.known or any(n in DF_METHODS for n in names[:-1])):
            self.risks["UNION_BY_POSITION"].append(f"{loc} .{m}() склеивает по позиции колонок, не по именам")
        elif m == "unionByName" and "allowMissingColumns" in kw:
            self.risks["UNION_MISSING_COLS"].append(loc)
        if m == "join" and (rid in self.known or any(n in DF_METHODS for n in names[:-1]) or isinstance(root, ast.Call)):
            how = self.sval(kw.get("how")) or (self.sval(a[2]) if len(a) > 2 else None) or "inner"
            if how in ("inner",):
                self.risks["INNER_JOIN"].append(f"{loc} join({short(a[0], 30) if a else '?'}, inner)")

    def collect_literals(self, col, expr, loc):
        for n in ast.walk(expr):
            if not isinstance(n, ast.Call) or not isinstance(n.func, ast.Attribute):
                continue
            vals = []
            if n.func.attr == "lit" and n.args:
                vals.append(n.args[0])
            elif n.func.attr == "when" and len(n.args) >= 2:
                vals.append(n.args[1])
            elif n.func.attr == "otherwise" and n.args:
                vals.append(n.args[0])
            for v in vals:
                if isinstance(v, ast.Constant) and isinstance(v.value, str) and v.value.strip():
                    self.literals[col].setdefault(v.value, loc)


# --------------------------------------------------------------------------- style

def style_stats(units, an):
    code_units = [u for u in units if u.kind in PIPELINE_KINDS and u.kind != "SQL" and not u.after_barrier]
    all_lines = []
    for u in code_units:
        all_lines += [l for l in u.lines]
    third = len(all_lines) * 2 // 3

    def chains(lines):
        bs = paren = 0
        prev = ""
        for l in lines:
            s = l.strip()
            if s and not s.startswith("#"):
                if s.endswith("\\"):
                    bs += 1
                elif s.startswith(".") and not prev.endswith("\\"):
                    paren += 1
            prev = s
        return bs, paren

    bs_all, par_all = chains(all_lines)
    bs_new, par_new = chains(all_lines[third:])
    text = "\n".join(all_lines)
    fcol = len(re.findall(r"\bF\.col\(", text))
    bare = len(re.findall(r"(?<![\w.])col\(", text))
    dq = sq = 0
    for u in code_units:
        try:
            for tok in tokenize.generate_tokens(io.StringIO(u.pycode()).readline):
                if tok.type == tokenize.STRING:
                    body = tok.string.lstrip("rbfuRBFU")
                    if body.startswith(('"""', "'''")):
                        continue
                    if body.startswith('"'):
                        dq += 1
                    elif body.startswith("'"):
                        sq += 1
        except (tokenize.TokenError, IndentationError, SyntaxError):
            pass
    out = [
        f"цепочки: через '\\' {bs_all} строк, в скобках {par_all} строк  |  в последней трети кода: '\\' {bs_new}, скобки {par_new}",
        f"колонки: F.col( ×{fcol}, голый col( ×{bare}  |  строки: \"…\" ×{dq}, '…' ×{sq}",
        f"withColumns ×{len(re.findall(r'\.withColumns\(', text))}, withColumnRenamed ×{len(re.findall(r'\.withColumnRenamed\(', text))}, "
        f".alias( ×{len(re.findall(r'\.alias\(', text))}, unionByName ×{len(re.findall(r'\.unionByName\(', text))}",
        f"комментарии: на русском ×{an.style['comment_ru']}, на английском ×{an.style['comment_en']}",
    ]
    if an.api:
        floor = max((v for v, _ in an.api.values()), key=lambda s: tuple(map(int, s.split("."))))
        out.append("API: " + ", ".join(f"{k}≥{v} ({l})" for k, (v, l) in an.api.items()) +
                   f"  →  в файле уже используется Spark ≥ {floor}")
    else:
        out.append("API: версионно-зависимых методов не найдено — в новом коде не использовать API новее Spark 3.0 без вопроса")
    return out


# --------------------------------------------------------------------------- output

def first_line(u):
    for l in u.lines:
        s = l.strip()
        if s and not s.startswith("%%") and not s.startswith("%"):
            return s
    return u.lines[0].strip() if u.lines else ""


def build_virtual(units, path, outdir):
    stem = Path(path).stem
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    vlines, olines = [], []
    for u in units:
        head = f"# %% [c{u.idx} {u.kind} id={u.cid or '-'} ex={u.exec if u.exec is not None else '-'}" \
               f"{' BARRIER' if u.barrier else ''}{' after-barrier' if u.after_barrier else ''}]"
        start = len(vlines) + 1
        vlines.append(head)
        vlines += u.lines if u.lines else [""]
        vlines.append("")
        u.vrange = (start, len(vlines) - 1)
        texts = []
        for o in u.outputs:
            t = o.get("output_type")
            if t == "stream":
                texts.append("".join(o.get("text", "")))
            elif t == "error":
                texts.append(f"ERROR {o.get('ename')}: {o.get('evalue')}")
            elif t in ("execute_result", "display_data"):
                d = o.get("data", {}).get("text/plain")
                d = "".join(d) if isinstance(d, list) else (d or "")
                if d and not WIDGET_RE.match(d):
                    texts.append(d)
        if texts:
            body = "\n".join(texts)
            if len(body) > 3000:
                body = body[:3000] + f"\n… [обрезано, всего {len(body)} символов]"
            olines.append(f"===== c{u.idx} ex={u.exec} | {clip(first_line(u), 70)}")
            olines.append(body.rstrip())
            olines.append("")
    vpath = outdir / f"{stem}.cells.py"
    opath = outdir / f"{stem}.outputs.txt"
    vpath.write_text("\n".join(vlines) + "\n", encoding="utf-8")
    opath.write_text(("\n".join(olines) if olines else "(полезных outputs нет)") + "\n", encoding="utf-8")
    return vpath, opath


def notebook_warnings(units, meta, an):
    w = []
    pipe = [u for u in units if u.kind in ("REMOTE", "PY", "SQL") and not u.after_barrier and not u.barrier]
    for i, u in enumerate(pipe):
        if u.exec is None and any(v.exec is not None for v in pipe[i + 1:]):
            w.append(f"c{u.idx} не выполнялась в сохранённой сессии, а следующие ячейки выполнялись — outputs ниже могли получиться без неё")
    prev = None
    for u in pipe:
        if u.exec is None:
            continue
        if prev is not None and u.exec < prev:
            w.append(f"c{u.idx} ex={u.exec} выполнялась раньше предыдущей ячейки (ex={prev}) — порядок запуска отличался от порядка ячеек")
        prev = max(prev or 0, u.exec)
    if meta.get("sparkmagic") and not meta.get("spark_kernel"):
        spark_names = {"spark", "F", "Window", "DeltaTable"} | an.known
        for u in units:
            if u.kind != "LOCAL" or u.barrier:
                continue
            if u.tree is None:
                u.parse()
            if u.tree is None:
                continue
            used = {n.id for n in ast.walk(u.tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
            hit = sorted(used & spark_names)
            if hit:
                w.append(f"c{u.idx} без %%spark выполнится ЛОКАЛЬНО, но использует {', '.join(hit[:5])} — вероятно, забыт магик")
    for u in units:
        if u.err and u.kind in ("REMOTE", "PY", "LOCAL"):
            w.append(f"синтаксическая ошибка: {u.err}")
    return w


def render_map(path, units, meta, an, full=False, emitted=None):
    out = []
    is_nb = path.endswith(".ipynb")
    code_units = [u for u in units if u.kind in ("REMOTE", "PY", "SQL", "LOCAL")]
    total_lines = sum(len(u.lines) for u in code_units)
    out.append(f"# PIPELINE MAP: {Path(path).name}")
    if is_nb:
        kinds = Counter(u.kind for u in units)
        out.append(f"ноутбук nbformat {meta['nbformat']} | kernel: {meta['kernel']} | sparkmagic: {'да' if meta['sparkmagic'] else 'нет'}"
                   f" | ячейки без магика выполняются: {meta['default_kind']}")
        out.append(f"ячеек {len(units)}: " + ", ".join(f"{k} {v}" for k, v in kinds.most_common()) + f" | строк кода {total_lines}")
        barrier = next((u for u in units if u.barrier), None)
        if barrier:
            out.append(f"БАРЬЕР: c{barrier.idx} ({clip(first_line(barrier), 30)}) — пайплайн = ячейки до него; ниже ручные/тестовые ячейки")
        if emitted:
            out.append(f"код ячеек: {emitted[0]}  |  outputs: {emitted[1]}")
    else:
        out.append(f"скрипт .py{' (экспорт Databricks)' if meta.get('databricks') else ''} | строк {total_lines}")
    if an.catalogs:
        out.append("каталоги в 3-частных именах: " + ", ".join(sorted(an.catalogs)) + " (ниже имена без каталога)")

    if is_nb:
        warns = notebook_warnings(units, meta, an)
        if warns:
            out.append("\n## ПРЕДУПРЕЖДЕНИЯ НОУТБУКА")
            out += [f"- {w}" for w in warns]
        out.append("\n## ЯЧЕЙКИ" + (" (v: строки в .cells.py)" if emitted else ""))
        for u in units:
            v = f" v:{u.vrange[0]}-{u.vrange[1]}" if u.vrange else ""
            ex = f" ex={u.exec}" if u.exec is not None else ""
            flag = " BARRIER" if u.barrier else (" ⚠после-барьера" if u.after_barrier and u.kind not in ("MD",) else "")
            out.append(f"c{u.idx:<3} {u.kind:<8} id={(u.cid or '-')[:8]}{ex}{v}{flag} | {clip(first_line(u), 70)}")
    else:
        errs = [u.err for u in units if u.err]
        if errs:
            out.append("\n## СИНТАКСИС")
            out += [f"- {e}" for e in errs]

    if an.params:
        out.append("\n## ПАРАМЕТРЫ И КОНСТАНТЫ")
        out += an.params
    if an.session:
        out.append("\n## НАСТРОЙКИ СЕССИИ")
        out += an.session
    if an.funcs:
        out.append("\n## ФУНКЦИИ")
        out += an.funcs

    out.append("\n## ТАБЛИЦЫ")
    for t, rec in an.tables.items():
        ins = ", ".join(rec["in"][:8]) + (f" …(+{len(rec['in']) - 8})" if len(rec["in"]) > 8 else "")
        outs = ", ".join(rec["out"][:8]) + (f" …(+{len(rec['out']) - 8})" if len(rec["out"]) > 8 else "")
        flag = "  ⚠test/temp" if re.search(r"(_test\b|^temp\.|^tmp)", t) and rec["out"] else ""
        out.append(f"{t}{flag}")
        if ins:
            out.append(f"    IN : {ins}")
        if outs:
            out.append(f"    OUT: {outs}")
    unk = {t: l for t, l in an.mentioned.items() if t not in an.tables}
    if unk:
        out.append("упомянуты строкой, направление не определено: " +
                   "; ".join(f"{t} ({', '.join(l[:4])})" for t, l in list(unk.items())[:20]))

    out.append("\n## ПОТОК (секции, DataFrame, записи — в порядке кода)")
    events = list(an.flow)
    for r in an.df_assigns:
        tag = ""
        if r["loop"]:
            tag += " ⟳цикл"
        if r["func"] and r["func"] != "main":
            tag += f" [def {r['func']}]"
        if r["unit"].after_barrier:
            tag += " ⚠после-барьера"
        ops = an.summarize(r["ops"], full=full, skip_source=r["skip_source"])
        line = f"  {r['loc']:<9} {r['name']} ← {r['root']}" + (f" | {ops}" if ops else "") + tag
        events.append((r["pos"], "df", line if full else clip(line, 230)))
    order = {"section": 0, "barrier": 0, "run": 1, "df": 2, "write": 3}
    for pos, kind, text in sorted(events, key=lambda e: (e[0], order.get(e[1], 9))):
        out.append(text if kind != "write" else "  " + text)

    if an.literals:
        out.append("\n## ЛИТЕРАЛЫ-КАТЕГОРИИ (значения, которые колонки получают через lit/when/otherwise)")
        items = sorted(an.literals.items(), key=lambda kv: -len(kv[1]))[:25]
        for col, vals in items:
            vs = ", ".join(f"{clip(repr(v), 40)}@{l}" for v, l in list(vals.items())[:10])
            more = f" …(+{len(vals) - 10})" if len(vals) > 10 else ""
            out.append(f"{col}: {vs}{more}")

    clones = [(k, v) for k, v in an.clones.items() if len(v) >= 2]
    if clones:
        out.append("\n## КЛОНЫ (одинаковые выражения в нескольких местах — правило придётся менять во всех копиях)")
        clones.sort(key=lambda kv: (-len(kv[1]), -len(kv[0])))
        for k, v in clones[:20]:
            cols = sorted({x[2] for x in v if x[2]})
            locs = " ".join(x[1] for x in sorted(v)[:12]) + (f" …(+{len(v) - 12})" if len(v) > 12 else "")
            out.append(f"×{len(v)} {clip(k, 110)}" + (f"  → [{', '.join(cols)[:60]}]" if cols else "") + f"\n     {locs}")

    if an.risks or (an.risks.get("CACHE") and not an.has_unpersist):
        out.append("\n## РИСКИ (сигналы для проверки, не обязательно ошибки)")
        for k in ["LOOP_DF", "CROSS_JOIN", "UNION_BY_POSITION", "ISIN_COLLECT", "WINDOW_NO_PARTITION", "DRIVER",
                  "UDF", "DEDUP_SUBSET", "SINGLE_PARTITION", "CACHE", "UNION_MISSING_COLS", "INNER_JOIN", "DEBUG_SHOW"]:
            v = an.risks.get(k)
            if not v:
                continue
            if k in ("DEBUG_SHOW", "UNION_MISSING_COLS", "CACHE", "INNER_JOIN"):
                note = {"DEBUG_SHOW": "активные .show()",
                        "UNION_MISSING_COLS": "unionByName(allowMissingColumns) — опечатка в имени колонки даст NULL, а не ошибку",
                        "CACHE": "cache/persist" + ("" if an.has_unpersist else " без unpersist в файле"),
                        "INNER_JOIN": "inner join — строки без пары молча пропадают"}[k]
                locs = list(OrderedDict.fromkeys(x.split(" ")[0] for x in v))
                out.append(f"{k} ×{len(v)}: {note}: {' '.join(locs[:15])}" + (" …" if len(locs) > 15 else ""))
            else:
                out.append(f"{k} ×{len(v)}:")
                out += [f"   {x}" for x in v[:10]] + ([f"   … (+{len(v) - 10})"] if len(v) > 10 else [])

    if an.guards:
        out.append("\n## ЗАЩИЩЁННЫЕ ЗОНЫ (не менять без явной просьбы)")
        seen = set()
        for g in an.guards:
            key = g.split(" ")[0]
            if key not in seen:
                seen.add(key)
                out.append(g)

    out.append("\n## СТИЛЬ")
    out += style_stats(units, an)
    if an.authors:
        out.append("подписи авторов в комментариях: " + ", ".join(f"{a} ×{n}" for a, n in an.authors.most_common(5)))
    if an.markers:
        out.append(f"маркеры правок ({len(an.markers)}), последние по коду:")
        out += [f"   {m}" for m in an.markers[-12:]]
    if an.refs:
        out.append("\n## ССЫЛКИ НА ЭТАЛОН / ДРУГУЮ РЕАЛИЗАЦИЮ")
        out += an.refs[:20]
    return "\n".join(out)


def render_df(an, name):
    out = [f"# DATAFRAME {name}"]
    recs = [r for r in an.df_assigns if r["name"] == name]
    if not recs:
        out.append("присваиваний не найдено (проверьте имя; параметры функций не отслеживаются как присваивания)")
    out.append("\n## ПРИСВАИВАНИЯ (каждое заменяет предыдущее состояние)")
    for r in recs:
        tag = (" ⟳цикл" if r["loop"] else "") + (f" [def {r['func']}]" if r["func"] and r["func"] != "main" else "")
        out.append(f"{r['loc']:<9} {name} ← {r['root']}{tag}")
        ops = an.summarize(r["ops"], full=True, skip_source=r["skip_source"])
        if ops:
            out.append("          " + ops)
    out.append("\n## ИСПОЛЬЗОВАНИЯ (во что этот DataFrame уходит)")
    for pos, loc, target, text in sorted(an.df_uses.get(name, [])):
        out.append(f"{loc:<9} → {target or '(выражение)'} | {clip(text, 120)}")
    return "\n".join(out)


def render_col(an, name):
    out = [f"# COLUMN {name}", "SET — создаётся/перезаписывается; RENAME — переименование; ALIAS — имя результата агрегата/select;",
           "DROP — удаляется; REF — используется; SQL — встречается в SQL-тексте. Стрелка → во что присваивается результат.", ""]
    if not an.col_events:
        out.append("не найдено (колонка может приходить из источника как есть — тогда она видна только в select/SQL источника)")
    for pos, loc, kind, target, text in sorted(an.col_events, key=lambda e: e[0]):
        out.append(f"{loc:<9} {kind:<8} → {target or '-':<22} | {text}")
    return "\n".join(out)


def run_check(path, units, meta, nb, an):
    problems, notes = [], []
    if nb is not None:
        if nb.get("nbformat") != 4:
            problems.append(f"nbformat={nb.get('nbformat')} (ожидается 4)")
        ids = []
        for i, c in enumerate(nb.get("cells", [])):
            ct = c.get("cell_type")
            if ct not in ("code", "markdown", "raw"):
                problems.append(f"c{i}: неизвестный cell_type={ct}")
            if not isinstance(c.get("source"), (str, list)):
                problems.append(f"c{i}: source должен быть строкой или списком строк")
            if isinstance(c.get("source"), list) and not all(isinstance(s, str) for s in c["source"]):
                problems.append(f"c{i}: в source не только строки")
            if not isinstance(c.get("metadata", {}), dict):
                problems.append(f"c{i}: metadata не словарь")
            if ct == "code":
                if "outputs" not in c or not isinstance(c.get("outputs"), list):
                    problems.append(f"c{i}: у code-ячейки нет списка outputs")
                if "execution_count" not in c:
                    problems.append(f"c{i}: у code-ячейки нет execution_count")
            if "id" in c:
                ids.append(c["id"])
                if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", str(c["id"])):
                    problems.append(f"c{i}: некорректный id={c['id']}")
        dup = [k for k, v in Counter(ids).items() if v > 1]
        if dup:
            problems.append("повторяющиеся id ячеек: " + ", ".join(dup))
        for w in notebook_warnings(units, meta, an):
            if "ЛОКАЛЬНО" in w or w.startswith("синтаксическая"):
                problems.append(w)
            else:
                notes.append(w)
    else:
        for u in units:
            if u.err:
                problems.append(f"синтаксис: {u.err}")
    out = [f"# CHECK {Path(path).name}"]
    if problems:
        out.append("ПРОБЛЕМЫ:")
        out += [f"- {p}" for p in problems]
    else:
        out.append("структура и синтаксис: OK")
    if notes:
        out.append("ПРЕДУПРЕЖДЕНИЯ:")
        out += [f"- {n}" for n in notes]
    return "\n".join(out), bool(problems)


def main():
    ap = argparse.ArgumentParser(description="Статическая карта PySpark .py/.ipynb")
    ap.add_argument("file")
    ap.add_argument("--df", help="история DataFrame")
    ap.add_argument("--col", help="история колонки")
    ap.add_argument("--emit", metavar="DIR", help="для .ipynb: записать код ячеек и outputs в DIR")
    ap.add_argument("--check", action="store_true", help="статическая проверка после правки")
    ap.add_argument("--full", action="store_true", help="не сокращать цепочки трансформаций")
    args = ap.parse_args()

    path = args.file
    nb = None
    try:
        if path.endswith(".ipynb"):
            units, meta, nb = load_ipynb(path)
        else:
            units, meta = load_py(path)
    except json.JSONDecodeError as e:
        print(f"# CHECK {Path(path).name}\nПРОБЛЕМЫ:\n- файл не является корректным JSON: {e}")
        sys.exit(1)

    an = Analyzer(units, want_col=args.col, want_df=args.df)
    an.run()

    emitted = None
    if args.emit and nb is not None:
        emitted = build_virtual(units, path, args.emit)

    if args.check:
        text, bad = run_check(path, units, meta, nb, an)
        print(text)
        sys.exit(1 if bad else 0)
    if args.df:
        print(render_df(an, args.df))
    elif args.col:
        print(render_col(an, args.col))
    else:
        print(render_map(path, units, meta, an, full=args.full, emitted=emitted))


if __name__ == "__main__":
    main()
