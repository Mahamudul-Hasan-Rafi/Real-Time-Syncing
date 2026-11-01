"""Reads a document query: its output fields, and which table columns feed each field.

Used for two things:
  * partial refresh: run the query with only the wanted output columns (no other sub-selects are evaluated);
  * audit: check the field map in config.yaml against what the SQL really reads.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import NamedTuple

_TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<lc>--[^\n]*)
  | (?P<bc>/\*.*?\*/)
  | (?P<str>'(?:[^']|'')*')
  | (?P<qid>"[^"]*")
  | (?P<num>\d+(?:\.\d+)?)
  | (?P<id>[A-Za-z_][\w$#]*(?:\s*\.\s*[A-Za-z_][\w$#]*)*)
  | (?P<op>.)
""", re.X | re.S)

_FROM_END = {"WHERE", "GROUP", "ORDER", "HAVING", "CONNECT", "START", "UNION", "MINUS", "INTERSECT"}


class ScanError(Exception):
    pass


class Tok(NamedTuple):
    kind: str          # id, str, num, op
    text: str
    start: int
    end: int


def tokenize(sql: str) -> list[Tok]:
    out = []
    for m in _TOKEN.finditer(sql):
        kind = m.lastgroup
        if kind in ("ws", "lc", "bc"):
            continue
        text = m.group()
        if kind == "id":
            text = re.sub(r"\s+", "", text)
        elif kind == "qid":
            kind, text = "id", text.strip('"')
        out.append(Tok(kind, text, m.start(), m.end()))
    return out


def _is(tok: Tok, text: str) -> bool:
    return tok.kind == "op" and tok.text == text


def _word(tok: Tok, word: str) -> bool:
    return tok.kind == "id" and tok.text.upper() == word


def _matching(toks: list[Tok], i: int) -> int:
    depth = 0
    for j in range(i, len(toks)):
        if _is(toks[j], "("):
            depth += 1
        elif _is(toks[j], ")"):
            depth -= 1
            if depth == 0:
                return j
    raise ScanError("unbalanced parenthesis")


def _top_level(toks: list[Tok], start: int, stop_words: set[str]) -> int:
    """Index of the first depth-0 id token in stop_words at or after start, else len(toks)."""
    depth = 0
    for j in range(start, len(toks)):
        t = toks[j]
        if _is(t, "("):
            depth += 1
        elif _is(t, ")"):
            depth -= 1
        elif depth == 0 and t.kind == "id" and t.text.upper() in stop_words:
            return j
    return len(toks)


def _split_commas(toks: list[Tok]) -> list[list[Tok]]:
    parts, cur, depth = [], [], 0
    for t in toks:
        if _is(t, "("):
            depth += 1
        elif _is(t, ")"):
            depth -= 1
        if depth == 0 and _is(t, ","):
            parts.append(cur)
            cur = []
        else:
            cur.append(t)
    parts.append(cur)
    return parts


@dataclass
class Item:
    name: str                 # output column name, lower case
    text: str                 # exact SQL text of the select item
    tokens: list
    alias_pos: list = field(default_factory=list)      # token positions that are the alias (and AS)


def _name_of(tokens: list[Tok]) -> tuple[str, list[int]]:
    if not tokens:
        raise ScanError("empty select item")
    last = tokens[-1]
    if last.kind != "id":
        raise ScanError(f"select item has no usable name: ...{last.text}")
    if "." in last.text:
        return last.text.rsplit(".", 1)[1].lower(), []
    if len(tokens) == 1:
        return last.text.lower(), []
    prev = tokens[-2]
    if _word(prev, "AS"):
        return last.text.lower(), [len(tokens) - 2, len(tokens) - 1]
    if prev.kind in ("id", "str", "num") or _is(prev, ")"):
        return last.text.lower(), [len(tokens) - 1]
    raise ScanError(f"select item has no alias: ...{prev.text} {last.text}")


@dataclass
class Select:
    sql: str
    toks: list
    items: list
    from_index: int

    @property
    def names(self) -> list[str]:
        seen, out = set(), []
        for it in self.items:
            if it.name not in seen:
                seen.add(it.name)
                out.append(it.name)
        return out

    def project(self, wanted: list[str]) -> str:
        """The same query returning only the wanted output columns. Everything after the select list is kept."""
        keep, chosen = set(wanted), []
        taken = set()
        for it in self.items:
            if it.name in keep and it.name not in taken:
                taken.add(it.name)
                chosen.append(it.text)
        missing = keep - taken
        if missing:
            raise KeyError(f"query has no output column {sorted(missing)}")
        return "SELECT " + ",\n       ".join(chosen) + "\n" + self.sql[self.toks[self.from_index].start:]


def parse_select(sql: str) -> Select:
    toks = tokenize(sql)
    if not toks or not _word(toks[0], "SELECT"):
        raise ScanError("query does not start with SELECT")
    from_index = _top_level(toks, 1, {"FROM"})
    if from_index >= len(toks):
        raise ScanError("no top-level FROM")
    items = []
    for part in _split_commas(toks[1:from_index]):
        name, alias_pos = _name_of(part)
        items.append(Item(name, sql[part[0].start:part[-1].end], part, alias_pos))
    return Select(sql, toks, items, from_index)


# ---------------------------------------------------------------------------------------------------
# column references
# ---------------------------------------------------------------------------------------------------
def _from_scope(from_tokens: list[Tok]) -> dict:
    alias, tables = {}, []
    for part in _split_commas(from_tokens):
        if not part or part[0].kind != "id":
            continue
        table = part[0].text.upper().rsplit(".", 1)[-1]
        tables.append(table)
        alias[table] = table
        rest = [t for t in part[1:] if not _word(t, "AS")]
        if rest and rest[-1].kind == "id":
            alias[rest[-1].text.upper()] = table
    return {"alias": alias, "tables": tables}


def _owner(name: str, chain: list, cols: dict) -> list:
    for scope in reversed(chain):
        hits = [t for t in scope["tables"] if name in cols.get(t, ())]
        if hits:
            return hits
    return []


def _is_alias(name: str, chain: list) -> bool:
    return any(name in scope["alias"] and scope["alias"][name] != name for scope in chain)


def _scan_tokens(toks: list[Tok], chain: list, cols: dict, out: set):
    i = 0
    while i < len(toks):
        t = toks[i]
        if _is(t, "("):
            j = _matching(toks, i)
            inner = toks[i + 1:j]
            if inner and _word(inner[0], "SELECT"):
                _scan_select(inner, chain, cols, out)
            else:
                _scan_tokens(inner, chain, cols, out)
            i = j + 1
            continue
        if t.kind == "id":
            nxt = toks[i + 1] if i + 1 < len(toks) else None
            if "." in t.text:
                prefix, col = t.text.upper().rsplit(".", 1)
                for scope in reversed(chain):
                    if prefix in scope["alias"]:
                        out.add((scope["alias"][prefix], col))
                        break
            elif not (nxt is not None and _is(nxt, "(")):
                name = t.text.upper()
                if not _is_alias(name, chain):
                    for table in _owner(name, chain, cols):
                        out.add((table, name))
        i += 1


def _scan_select(toks: list[Tok], chain: list, cols: dict, out: set):
    from_idx = _top_level(toks, 1, {"FROM"})
    if from_idx >= len(toks):
        _scan_tokens(toks[1:], chain, cols, out)
        return
    stop = _top_level(toks, from_idx + 1, _FROM_END)
    scope = _from_scope(toks[from_idx + 1:stop])
    inner = chain + [scope]
    _scan_tokens(toks[1:from_idx], inner, cols, out)
    _scan_tokens(toks[stop:], inner, cols, out)


@dataclass
class Refs:
    fields: dict            # output field -> {(TABLE, COLUMN)}
    join: set               # (TABLE, COLUMN) used by the top-level join / filter
    top_tables: list        # tables of the top-level FROM


def column_refs(select: Select, cols: dict) -> Refs:
    """cols: TABLE -> set of COLUMN names (upper case), for every table the query can touch."""
    toks = select.toks
    stop = _top_level(toks, select.from_index + 1, _FROM_END)
    scope = _from_scope(toks[select.from_index + 1:stop])
    chain = [scope]
    fields: dict = {}
    for item in select.items:
        drop = set(item.alias_pos)
        body = [t for k, t in enumerate(item.tokens) if k not in drop]
        refs: set = set()
        _scan_tokens(body, chain, cols, refs)
        fields.setdefault(item.name, set()).update(refs)
    join: set = set()
    _scan_tokens(toks[stop:], chain, cols, join)
    return Refs(fields, join, scope["tables"])
