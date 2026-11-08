"""Oracle access: rebuild documents for a set of keys, resolve lookup queries, and the start-up probes."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import oracledb

from ..core.model import IndexSpec, Key
from ..settings import OracleSettings

oracledb.defaults.fetch_lobs = False      # CLOB columns (the NOMINEE JSON) come back as str

_TRANSIENT_CODES = {
    "ORA-03113", "ORA-03114", "ORA-03135", "ORA-03136", "ORA-12170", "ORA-12514", "ORA-12541",
    "ORA-12543", "ORA-12571", "ORA-01033", "ORA-01034", "ORA-01089", "ORA-01090", "ORA-00020",
    "ORA-02396", "ORA-12537", "ORA-12547",
    "DPY-4011", "DPY-4024", "DPY-1001", "DPY-6000", "DPY-6001", "DPY-6003", "DPY-6005",
}

Row = dict[str, Any]


def is_transient(exc: BaseException) -> bool:
    """True for failures that a retry can cure: lost connection, database restart, timeouts."""
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    if isinstance(exc, oracledb.Error):
        err = exc.args[0] if exc.args else None
        code = getattr(err, "full_code", "") or ""
        if code in _TRANSIENT_CODES:
            return True
        return isinstance(exc, (oracledb.OperationalError, oracledb.InterfaceError)) and not code.startswith("ORA-0")
    return False


def key_condition(spec: IndexSpec, keys: list[Key]) -> tuple[str, dict[str, Any]]:
    """AND-clause matching a chunk of keys, with bind variables."""
    binds: dict[str, Any] = {}
    if len(spec.key_columns) == 1:
        names = []
        for i, key in enumerate(keys):
            binds[f"k{i}"] = key[0]
            names.append(f":k{i}")
        return f"{spec.key_filter[0]} IN ({', '.join(names)})", binds
    groups = []
    for i, key in enumerate(keys):
        names = []
        for j, value in enumerate(key):
            binds[f"k{i}_{j}"] = value
            names.append(f":k{i}_{j}")
        groups.append(f"({', '.join(names)})")
    # A tuple IN-list alone makes Oracle scan the table (500 keys: 4 s). A plain IN-list on the first key column
    # in front of it lets Oracle narrow the rows first (0.2 s); the tuple list then picks the exact rows.
    firsts = list(dict.fromkeys(key[0] for key in keys))
    first_names = []
    for i, value in enumerate(firsts):
        binds[f"f{i}"] = value
        first_names.append(f":f{i}")
    return (f"{spec.key_filter[0]} IN ({', '.join(first_names)}) "
            f"AND ({', '.join(spec.key_filter)}) IN ({', '.join(groups)})"), binds


class OracleReader:
    """A lazily created connection pool. Every query goes through `cursor()`."""

    def __init__(self, settings: OracleSettings):
        self.settings = settings
        self.pool: oracledb.ConnectionPool | None = None

    def connect(self) -> None:
        s = self.settings
        self.pool = oracledb.create_pool(
            user=s.user, password=s.password, dsn=s.dsn,
            min=s.pool_min, max=s.pool_max, increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT, ping_interval=30, tcp_connect_timeout=10,
        )

    def close(self) -> None:
        if self.pool is not None:
            self.pool.close(force=True)
            self.pool = None

    @contextmanager
    def cursor(self) -> Iterator[oracledb.Cursor]:
        if self.pool is None:
            self.connect()
        assert self.pool is not None
        with self.pool.acquire() as conn:
            conn.call_timeout = self.settings.call_timeout_ms
            with conn.cursor() as cur:
                cur.arraysize = self.settings.fetch_array_size
                yield cur

    @staticmethod
    def _names(cur: oracledb.Cursor) -> list[str]:
        """Lower-case output column names of the executed statement."""
        return [d[0].lower() for d in cur.description or []]

    @classmethod
    def _dicts(cls, cur: oracledb.Cursor, rows: list[Any]) -> list[Row]:
        names = cls._names(cur)
        return [dict(zip(names, r)) for r in rows]

    # -- document reads -----------------------------------------------------------------------------------------
    def fetch_rows(self, spec: IndexSpec, keys: list[Key]) -> list[Row]:
        """The document query restricted to these keys."""
        condition, binds = key_condition(spec, keys)
        with self.cursor() as cur:
            cur.execute(f"{spec.sql}\n AND {condition}", binds)
            return self._dicts(cur, cur.fetchall())

    def fetch_fields(self, spec: IndexSpec, keys: list[Key], fields: list[str]) -> list[Row]:
        """Only the key columns and the wanted output fields of the document query, for these keys."""
        condition, binds = key_condition(spec, keys)
        wanted = sorted(set(fields) | set(spec.key_names))
        with self.cursor() as cur:
            cur.execute(f"{spec.select.project(wanted)}\n AND {condition}", binds)
            return self._dicts(cur, cur.fetchall())

    def lookup_chunks(self, sql: str, params: dict[str, Any], chunk: int) -> Iterator[list[tuple]]:
        """Yields lists of key tuples returned by a lookup query."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(chunk)
                if not rows:
                    return
                yield [tuple(r) for r in rows]

    def stream_all(self, spec: IndexSpec, chunk: int) -> Iterator[list[Row]]:
        """Every row of the document query, in chunks of dict rows (full re-sync)."""
        with self.cursor() as cur:
            cur.execute(spec.sql)
            names = self._names(cur)
            while True:
                rows = cur.fetchmany(chunk)
                if not rows:
                    return
                yield [dict(zip(names, r)) for r in rows]

    # -- start-up checks ----------------------------------------------------------------------------------------
    def ping(self) -> None:
        with self.cursor() as cur:
            cur.execute("SELECT 1 FROM dual")
            cur.fetchone()

    def compile_check(self, sql: str, binds: dict[str, Any] | None = None) -> None:
        """Runs a statement that returns no rows, to prove it parses and the columns exist."""
        with self.cursor() as cur:
            cur.execute(sql, binds or {})
            cur.fetchall()

    def output_names(self, sql: str) -> list[str]:
        """Column names a statement returns (it is run with no rows)."""
        with self.cursor() as cur:
            cur.execute(sql)
            names = self._names(cur)
            cur.fetchall()
            return names

    def table_columns(self, schema: str, tables: list[str]) -> dict[str, set[str]]:
        """table -> set of column names."""
        found: dict[str, set[str]] = {}
        with self.cursor() as cur:
            cur.execute("SELECT table_name, column_name FROM all_tab_columns WHERE owner = :o", {"o": schema})
            wanted = {t.upper() for t in tables}
            for table, column in cur.fetchall():
                if table.upper() in wanted:
                    found.setdefault(table.upper(), set()).add(column.upper())
        return found

    def log_group_columns(self, schema: str) -> dict[str, set[str]]:
        """table -> set of columns held in ALWAYS user log groups."""
        found: dict[str, set[str]] = {}
        with self.cursor() as cur:
            cur.execute(
                "SELECT g.table_name, c.column_name FROM all_log_groups g "
                "JOIN all_log_group_columns c ON c.owner = g.owner AND c.log_group_name = g.log_group_name "
                "WHERE g.owner = :o AND g.always = 'ALWAYS'", {"o": schema})
            for table, column in cur.fetchall():
                found.setdefault(table.upper(), set()).add(column.upper())
        return found
