"""
Tables to validate from two queries instead of a `tables:` list in the YAML.

    table_source:
      tables_query: SELECT table_name, load_type, incr_column, watermark FROM <control table>
      keys_query:   SELECT table_name, key_status, key_list FROM it_bronze_test.audit.talend_dpi_keys
    overrides:                       # optional, per table ('*' = every table)
      BLSBSH: {exclude_cols: [CHANGE_TIMESTAMP]}

Per table (tables_query LEFT JOIN keys_query on the table name, upper case, without catalog / schema):
  keys         key_status = pk_value ('PRIMARY KEY')  -> key_list split on ','     otherwise: no keys (whole-row)
  load type    in full_values         -> full comparison of the whole table
               in incremental_values  -> incremental: <incr_column> BETWEEN '<watermark day> 00:00:00'
                                         AND '<watermark>' on source and target, delta_only (default true)
  exclude_cols table_source.exclude_cols + overrides (a key column is never excluded)
Overrides: any table option (keys, exclude_cols, num_tolerance, ts_truncate, target_name, filter, tolerances, ...);
  keys replace the queried keys, exclude_cols are added to the global ones, incremental: {...} is merged
  (e.g. {delta_only: false} = full comparison of the table as well).
"""
from __future__ import annotations

import re
from typing import Optional

from .delta_yaml import _fmt, _window

DEFAULT_COLUMNS = dict(table_name="table_name", load_type="load_type", incr_column="incr_column",
                       watermark="watermark", key_status="key_status", key_list="key_list")
ALLOWED = {"tables_query", "keys_query", "columns", "pk_value", "key_separator", "full_values",
           "incremental_values", "delta_only", "exclude_cols", "window_start"}
TABLE_POS = ("table_name", "load_type", "incr_column", "watermark")      # fall-back: column positions
KEY_POS = ("table_name", "key_status", "key_list")
_DB2_TS = re.compile(r"^(\d{4}-\d{2}-\d{2})-(\d{2})\.(\d{2})\.(\d{2})(\.\d+)?$")


def check(ts: dict):
    """Validate the table_source block (called by load_config; reads no data)."""
    if not isinstance(ts, dict):
        raise ValueError("table_source must be a block with tables_query / keys_query")
    unknown = set(ts) - ALLOWED
    if unknown:
        raise ValueError(f"table_source: unknown option(s) {sorted(unknown)}; allowed: {sorted(ALLOWED)}")
    if not ts.get("tables_query"):
        raise ValueError("table_source needs tables_query (table name, load type, incremental column, watermark)")
    bad = set(ts.get("columns") or {}) - set(DEFAULT_COLUMNS)
    if bad:
        raise ValueError(f"table_source.columns: unknown name(s) {sorted(bad)}; allowed: {sorted(DEFAULT_COLUMNS)}")


def _name(v) -> str:
    return str(v or "").strip().split(".")[-1].upper()


def _wm(v) -> str:
    """Watermark as Spark SQL timestamp text; DB2 text 2024-10-24-11.36.37.123456 -> 2024-10-24 11:36:37.123456."""
    s = _fmt(v)
    m = _DB2_TS.match(s)
    return f"{m.group(1)} {m.group(2)}:{m.group(3)}:{m.group(4)}{m.group(5) or ''}" if m else s


def _collect(spark, query: str, wanted: tuple, names: dict, what: str) -> list:
    """Rows as dicts with the framework's names; a column is found by its configured name, else by position."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    df = spark.sql(query)
    timeish = (T.TimestampType, T.DateType) + ((T.TimestampNTZType,) if hasattr(T, "TimestampNTZType") else ())
    # timestamps as text inside Spark: collected as Python datetimes they would move to the driver's time zone
    df = df.select(*[F.col(f"`{f.name}`").cast("string").alias(f.name) if isinstance(f.dataType, timeish)
                     else F.col(f"`{f.name}`") for f in df.schema.fields])
    lower = {c.lower(): i for i, c in enumerate(df.columns)}
    idx = {}
    for pos, key in enumerate(wanted):
        col = str(names.get(key, key)).lower()
        if col in lower:
            idx[key] = lower[col]
        elif pos < len(df.columns):
            idx[key] = pos
            print(f"  table_source.{what}: no column '{col}', using column {pos + 1} ({df.columns[pos]}) as {key}")
        else:
            raise ValueError(f"table_source.{what} returns {len(df.columns)} column(s); {key} is missing "
                             f"(set table_source.columns.{key})")
    return [{k: r[i] for k, i in idx.items()} for r in df.collect()]


def build_tables(spark, ts: dict, overrides: Optional[dict] = None, load_type: Optional[str] = None,
                 only: Optional[set] = None) -> tuple:
    """
    Table dicts (the same shape as a YAML `tables:` entry) from the two queries.
    load_type: None = every table; 'full' / 'incremental' (or any value of full_values / incremental_values).
    Returns (tables, skipped) with skipped = [(table, reason)].
    """
    names = {**DEFAULT_COLUMNS, **{k: v for k, v in (ts.get("columns") or {}).items() if v}}
    full_vals = {str(v).strip().upper() for v in (ts.get("full_values") or ["FULL"])}
    inc_vals = {str(v).strip().upper() for v in (ts.get("incremental_values") or ["INCREMENTAL", "INCR", "DELTA"])}
    pk_value = str(ts.get("pk_value", "PRIMARY KEY")).strip().upper()
    sep = ts.get("key_separator", ",")
    delta_only = bool(ts.get("delta_only", True))
    global_excl = [str(c).upper() for c in (ts.get("exclude_cols") or [])]
    ov = {str(k).upper(): (v or {}) for k, v in (overrides or {}).items()}

    want = None
    if load_type:
        lt = str(load_type).strip().upper()
        want = "FULL" if lt in full_vals or lt == "FULL" else "INCREMENTAL" if lt in inc_vals or lt.startswith("INC") \
            else None
        if not want:
            raise ValueError(f"load_type must be full or incremental (or one of {sorted(full_vals | inc_vals)}), "
                             f"got {load_type!r}")

    # keys: one row per table; a PRIMARY KEY row wins over any other row of the same table
    keys = {}
    if ts.get("keys_query"):
        for r in _collect(spark, ts["keys_query"], KEY_POS, names, "keys_query"):
            name = _name(r["table_name"])
            if not name:
                continue
            is_pk = str(r["key_status"] or "").strip().upper() == pk_value
            cols = [c.strip().upper() for c in str(r["key_list"] or "").split(sep) if c.strip()] if is_pk else []
            if name not in keys or (cols and not keys[name]):
                keys[name] = cols

    tables, skipped, seen = [], [], set()
    for r in _collect(spark, ts["tables_query"], TABLE_POS, names, "tables_query"):
        name = _name(r["table_name"])
        if not name or (only and name not in only):
            continue
        if name in seen:
            skipped.append((name, "listed more than once in tables_query: first row used"))
            continue
        seen.add(name)
        lt = str(r["load_type"] or "").strip().upper()
        kind = "FULL" if lt in full_vals else "INCREMENTAL" if lt in inc_vals else None
        if kind is None:                                        # reported once: in the full (or unfiltered) run
            if want == "INCREMENTAL":
                continue
            skipped.append((name, f"load type {r['load_type']!r} is neither {sorted(full_vals)} nor {sorted(inc_vals)}"))
            continue
        if want and kind != want:
            continue
        o = dict(ov.get("*") or {})
        for k, v in (ov.get(name) or {}).items():              # table override on top of '*'
            o[k] = {**(o.get(k) or {}), **v} if k == "incremental" and isinstance(v, dict) else v

        entry = {"name": name, "keys": list(keys.get(name, []))}
        if name not in keys and ts.get("keys_query"):
            print(f"  {name}: not in keys_query - compared without keys")
        if o.get("keys") is not None:
            entry["keys"] = [str(k).upper() for k in o.pop("keys")]
        excl = global_excl + [str(c).upper() for c in (o.pop("exclude_cols", None) or [])]
        entry["exclude_cols"] = [c for c in dict.fromkeys(excl) if c not in entry["keys"]]

        inc_over = o.pop("incremental", None)
        if kind == "INCREMENTAL":
            col, wm = str(r["incr_column"] or "").strip(), r["watermark"]
            if not col or wm is None or not str(wm).strip():
                skipped.append((name, "incremental table without incremental column / watermark in tables_query"))
                continue
            wm = _wm(wm)
            start = ts.get("window_start")
            entry["incremental"] = {"source_delta": _window(col, wm, start), "target_delta": _window(col, wm, start),
                                    "delta_only": delta_only, **(inc_over or {})}
        elif inc_over:
            print(f"  {name}: full-load table, overrides.incremental ignored")
        entry.update(o)                                        # tolerances, ts_truncate, target_name, filter, ...
        tables.append(entry)
    for n in sorted(set(ov) - seen - {"*"}):
        if not only or n in only:
            print(f"  overrides.{n}: table not in tables_query (ignored)")
    return tables, skipped


def describe(tables: list, skipped: list, load_type: Optional[str]) -> str:
    lines = [f"tables from table_source ({load_type or 'all load types'}): {len(tables)} to validate"
             + (f", {len(skipped)} skipped" if skipped else "")]
    for t in tables:
        inc = t.get("incremental")
        lines.append(f"  {t['name']:<24} {'INCREMENTAL' if inc else 'FULL':<12} keys {t['keys'] or '[] (whole-row)'}"
                     + (f"   {inc['source_delta']}" + ("" if inc.get('delta_only', True) else "  (+ full table)")
                        if inc else ""))
    for n, why in skipped:
        lines.append(f"  {n:<24} SKIPPED      {why}")
    return "\n".join(lines)
