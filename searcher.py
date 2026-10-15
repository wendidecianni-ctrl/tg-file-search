"""File indexing + search.

- Incremental SQLite FTS5 index (only new/changed files are re-read)
- Readers: txt/md/log, csv, xlsx, xls, docx, pdf
- Multi-word search (all words must appear in the same line/row/paragraph/page),
  "quoted phrases", prefix matching, and automatic fuzzy fallback for typos.
"""
import csv
import html
import json
import logging
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import xlrd
from docx import Document
from openpyxl import load_workbook
from pypdf import PdfReader
from rapidfuzz import fuzz, process

log = logging.getLogger("searcher")

HL_START, HL_END = "\x01", "\x02"   # snippet highlight markers (swapped for <b> later)


# --------------------------------------------------------------------- readers
def read_txt(p):
    with open(p, encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, 1):
            yield f"line {i}", line.strip()


def _table(rows, prefix=""):
    """First non-empty row = header. Later rows also yield {header: value} for filters."""
    headers = None
    for i, raw in enumerate(rows, 1):
        cells = ["" if c is None else str(c).strip() for c in raw]
        if not any(cells):
            continue
        text = " | ".join(c for c in cells if c)
        if headers is None:
            headers = [c or f"col{n}" for n, c in enumerate(cells, 1)]
            yield f"{prefix}row {i}", text
        else:
            yield f"{prefix}row {i}", text, {h: c for h, c in zip(headers, cells) if c}


def read_csv(p):
    with open(p, encoding="utf-8", errors="ignore", newline="") as f:
        yield from _table(csv.reader(f))


def read_xlsx(p):
    wb = load_workbook(p, read_only=True, data_only=True)
    try:
        for ws in wb.worksheets:
            yield from _table(ws.iter_rows(values_only=True), prefix=f"{ws.title}, ")
    finally:
        wb.close()


def read_xls(p):
    wb = xlrd.open_workbook(p)
    for ws in wb.sheets():
        yield from _table((ws.row_values(i) for i in range(ws.nrows)),
                          prefix=f"{ws.name}, ")


def read_docx(p):
    doc = Document(p)
    for i, para in enumerate(doc.paragraphs, 1):
        yield f"paragraph {i}", para.text
    for t, table in enumerate(doc.tables, 1):
        for r, row in enumerate(table.rows, 1):
            yield f"table {t}, row {r}", " | ".join(c.text for c in row.cells)


def read_pdf(p):
    reader = PdfReader(p)
    if reader.is_encrypted:
        try:
            reader.decrypt("")          # many "locked" PDFs open with an empty password
        except Exception:
            raise ValueError("PDF is password-protected")
    for n, page in enumerate(reader.pages, 1):
        text = page.extract_text() or ""
        yield f"page {n}", re.sub(r"\s+", " ", text).strip()


READERS = {
    ".txt": read_txt, ".md": read_txt, ".log": read_txt,
    ".csv": read_csv,
    ".xlsx": read_xlsx, ".xls": read_xls,
    ".docx": read_docx,
    ".pdf": read_pdf,
}

# ----------------------------------------------------------------- query parsing
QUERY_RE = re.compile(r'([^\W\d]\w*):(?:"([^"]+)"|(\w+))|"([^"]+)"|(\w+)')


def parse_query(q):
    """'invoice "john smith" city:accra' ->
       terms   [('word','invoice'), ('phrase',['john','smith'])]
       filters [('city','accra')]"""
    terms, filters = [], []
    for key, qv, v, phrase, word in QUERY_RE.findall(q):
        if key:
            filters.append((key.lower(), (qv or v).lower()))
        elif phrase:
            toks = re.findall(r"\w+", phrase.lower())
            if toks:
                terms.append(("phrase", toks))
        elif word:
            terms.append(("word", word.lower()))
    return terms, filters


def _tok_match(t, ct, fuzzy):
    if t == ct:
        return True
    return (fuzzy and len(t) >= 4 and not t.isdigit()
            and fuzz.ratio(t, ct) >= (80 if len(t) <= 5 else 75))


def _header_matches(key, header):
    """'first' matches 'First Name'; 'city' does NOT match 'Ethnicity'."""
    nk = re.sub(r"[\W_]+", "", key)
    if nk == re.sub(r"[\W_]+", "", header.lower()):
        return True
    return any(w.startswith(nk) for w in re.findall(r"[^\W_]+", header.lower()))


def _filters_ok(fields_json, filters, fuzzy):
    if not fields_json:
        return False
    fields = json.loads(fields_json)
    for key, value in filters:
        vtoks = re.findall(r"\w+", value)
        ok = False
        for header, cell in fields.items():
            if _header_matches(key, header):
                ctoks = re.findall(r"\w+", cell.lower())
                if all(any(_tok_match(t, ct, fuzzy) for ct in ctoks) for t in vtoks):
                    ok = True
                    break
        if not ok:
            return False
    return True


BATCH = 20_000                 # rows per commit while indexing (keeps RAM flat)
SCHEMA = """
CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY, mtime REAL, size INTEGER);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks USING fts5(
    file UNINDEXED, loc UNINDEXED, text,
    tokenize = 'unicode61 remove_diacritics 2');
CREATE TABLE IF NOT EXISTS cols(file TEXT, header TEXT);
CREATE TABLE IF NOT EXISTS row_fields(file TEXT, loc TEXT, fields TEXT,
                                     PRIMARY KEY(file, loc));
CREATE VIRTUAL TABLE IF NOT EXISTS vocab USING fts5vocab(chunks, 'row');
"""


class Index:
    def __init__(self, folder, db_path):
        self.folder = Path(folder)
        self.db_path = str(db_path)
        self._locks_guard = threading.Lock()
        self._file_locks = {}            # one lock per file, so uploads never wait on a big sync
        self._vocab = None               # cached word list for fuzzy matching
        self._enable_wal()
        with self._conn() as c:
            # Add auxiliary metadata tables without replacing existing FTS data.
            c.executescript(SCHEMA)
            self._fts_fields = "fields" in {
                row[1] for row in c.execute("PRAGMA table_info(chunks)")
            }

    def _enable_wal(self):
        deadline = time.monotonic() + 30
        while True:
            try:
                with self._conn(timeout=0.1) as c:
                    mode = c.execute("PRAGMA journal_mode").fetchone()[0]
                    if mode.lower() == "wal":
                        return
                    c.execute("PRAGMA journal_mode=WAL")
                return
            except sqlite3.OperationalError as exc:
                if not any(w in str(exc).lower() for w in ("locked", "busy")):
                    raise
                if time.monotonic() >= deadline:
                    raise sqlite3.OperationalError(
                        f"Timed out enabling WAL for {self.db_path}; "
                        "the database is still locked by another connection") from exc
                time.sleep(0.1)

    @contextmanager
    def _conn(self, timeout=30):
        c = sqlite3.connect(self.db_path, timeout=timeout)
        c.execute("PRAGMA synchronous=NORMAL")      # safe with WAL, much faster bulk inserts
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    # ------------------------------------------------------------------ indexing
    def _file_lock(self, rel):
        with self._locks_guard:
            return self._file_locks.setdefault(rel, threading.Lock())

    def index_file(self, path):
        """(Re)index one file. Returns number of text entries, or None on failure.
        Streams rows in batches, so even a 20-million-line file uses little RAM and
        never holds the database locked for long."""
        path = Path(path)
        reader = READERS.get(path.suffix.lower())
        if not reader:
            return None
        rel = str(path.relative_to(self.folder))
        with self._file_lock(rel):
            try:
                st = path.stat()
                with self._conn() as c:
                    known = c.execute("SELECT 1 FROM files WHERE path=?", (rel,)).fetchone()
                    if known:       # replacing old data (the DELETE scans the table: skip it for new files)
                        c.execute("DELETE FROM chunks WHERE file=?", (rel,))
                        c.execute("DELETE FROM cols WHERE file=?", (rel,))
                        c.execute("DELETE FROM row_fields WHERE file=?", (rel,))
                    # marker: if we get interrupted, the next sync() redoes this file cleanly
                    c.execute("INSERT OR REPLACE INTO files VALUES (?,?,?)", (rel, 0, -1))
                total, headers, batch = 0, {}, []
                for item in reader(path):
                    loc, text = item[0], item[1]
                    fields = item[2] if len(item) > 2 else None
                    if text and text.strip():
                        batch.append((rel, loc, text,
                                      json.dumps(fields, ensure_ascii=False) if fields else None))
                        if fields:
                            headers.update(dict.fromkeys(fields))
                        if len(batch) >= BATCH:
                            total += self._insert(batch)
                            batch = []
                total += self._insert(batch)
                with self._conn() as c:
                    c.executemany("INSERT INTO cols VALUES (?,?)", [(rel, h) for h in headers])
                    c.execute("INSERT OR REPLACE INTO files VALUES (?,?,?)",
                              (rel, st.st_mtime, st.st_size))
            except Exception:
                log.exception("Could not index %s", rel)
                return None
        self._vocab = None
        log.info("Indexed %s (%d entries)", rel, total)
        return total

    def _insert(self, batch):
        if batch:
            with self._conn() as c:
                if self._fts_fields:
                    c.executemany(
                        "INSERT INTO chunks(file, loc, text, fields) VALUES (?,?,?,?)",
                        batch,
                    )
                else:
                    c.executemany(
                        "INSERT INTO chunks(file, loc, text) VALUES (?,?,?)",
                        [(file, loc, text) for file, loc, text, _ in batch],
                    )
                    c.executemany(
                        "INSERT OR REPLACE INTO row_fields VALUES (?,?,?)",
                        [(file, loc, fields) for file, loc, _, fields in batch
                         if fields],
                    )
        return len(batch)

    def remove_file(self, rel):
        with self._file_lock(rel), self._conn() as c:
            c.execute("DELETE FROM chunks WHERE file=?", (rel,))
            c.execute("DELETE FROM cols WHERE file=?", (rel,))
            c.execute("DELETE FROM row_fields WHERE file=?", (rel,))
            c.execute("DELETE FROM files WHERE path=?", (rel,))
        self._vocab = None

    def sync(self):
        """Bring the index in line with the folder. Returns (added/updated, removed)."""
        on_disk = {str(p.relative_to(self.folder)): p
                   for p in self.folder.rglob("*")
                   if p.is_file() and p.suffix.lower() in READERS}
        with self._conn() as c:
            known = {r[0]: (r[1], r[2])
                     for r in c.execute("SELECT path, mtime, size FROM files")}
        removed = known.keys() - on_disk.keys()
        for rel in removed:
            self.remove_file(rel)
        todo = []
        for rel, p in on_disk.items():
            st = p.stat()
            if known.get(rel) == (st.st_mtime, st.st_size):
                continue
            todo.append(p)
        log.info("Sync: %d file(s) to index, %d to remove", len(todo), len(removed))
        changed = 0
        for n, p in enumerate(todo, 1):
            log.info("Indexing %d/%d: %s", n, len(todo), p.name)
            if self.index_file(p) is not None:
                changed += 1
        log.info("Sync done: %d indexed, %d removed", changed, len(removed))
        return changed, len(removed)

    # -------------------------------------------------------------------- search
    def _vocab_terms(self):
        if self._vocab is None:
            # Only keep real words of 4-25 letters (codes, numbers and long IDs can't be
            # fuzzy-matched anyway). This keeps the cache small, and building it is a
            # one-off: it is reused by every search until a file changes.
            with self._conn() as c:
                self._vocab = [t for (t,) in c.execute(
                    "SELECT term FROM vocab WHERE length(term) BETWEEN 4 AND 25")
                    if t.isalpha()]
        return self._vocab

    def _fuzzy_variants(self, word):
        if word.isdigit() or len(word) < 4:
            return [word]
        cutoff = 80 if len(word) <= 5 else 75
        hits = process.extract(word, self._vocab_terms(), scorer=fuzz.ratio,
                               score_cutoff=cutoff, limit=6)
        return list(dict.fromkeys([word] + [h[0] for h in hits]))

    def _build_match(self, terms, fuzzy):
        parts = []
        for kind, val in terms:
            if kind == "phrase":
                parts.append('"%s"' % " ".join(val))
            elif fuzzy:
                variants = self._fuzzy_variants(val)
                parts.append("(" + " OR ".join(f'"{v}"' for v in variants) + ")")
            else:
                parts.append(f'"{val}"*')          # prefix match: "invo" finds "invoice"
        return " AND ".join(parts)

    def _fmt(self, snip):
        return html.escape(snip).replace(HL_START, "<b>").replace(HL_END, "</b>")

    def _run(self, match, limit):
        """No filters: ranked hits with highlighted snippets."""
        sql = ("SELECT file, loc, snippet(chunks, 2, char(1), char(2), '…', 24) "
               "FROM chunks WHERE chunks MATCH ? ORDER BY rank LIMIT ?")
        try:
            with self._conn() as c:
                return [(f, w, self._fmt(sn)) for f, w, sn in c.execute(sql, (match, limit))]
        except sqlite3.OperationalError:
            log.exception("Bad FTS query: %s", match)
            return []

    def _run_filtered(self, match, filters, fuzzy, limit):
        """With filters: check columns on many candidates first (cheap), and only
        build snippets (expensive) for the rows we actually keep."""
        try:
            with self._conn() as c:
                if self._fts_fields:
                    fields_select = "chunks.fields"
                    fields_join = ""
                else:
                    fields_select = "row_fields.fields"
                    fields_join = (
                        "LEFT JOIN row_fields "
                        "ON row_fields.file=chunks.file AND row_fields.loc=chunks.loc"
                    )
                cands = c.execute(
                    "SELECT chunks.rowid, chunks.file, chunks.loc, "
                    f"{fields_select} FROM chunks {fields_join} "
                    "WHERE chunks MATCH ? ORDER BY rank LIMIT 5000",
                    (match,),
                ).fetchall()
                keep = [r for r in cands if _filters_ok(r[3], filters, fuzzy)][:limit]
                if not keep:
                    return []
                marks = ",".join("?" * len(keep))
                snips = dict(c.execute(
                    "SELECT rowid, snippet(chunks, 2, char(1), char(2), '…', 24) FROM chunks "
                    f"WHERE chunks MATCH ? AND rowid IN ({marks})",
                    (match, *[r[0] for r in keep])))
        except sqlite3.OperationalError:
            log.exception("Bad FTS query: %s", match)
            return []
        return [(f, w, self._fmt(snips.get(rid, ""))) for rid, f, w, _ in keep]

    def _search(self, terms, filters, fuzzy, limit):
        # Filter values also go into the FTS query to narrow candidates quickly;
        # the exact column check happens afterwards.
        fts_terms = terms + [("word", t) for _, v in filters for t in re.findall(r"\w+", v)]
        if not fts_terms:
            return []
        match = self._build_match(fts_terms, fuzzy)
        if filters:
            return self._run_filtered(match, filters, fuzzy, limit)
        return self._run(match, limit)

    def search(self, query, limit=200):
        """Returns (results, used_fuzzy). results = [(file, location, snippet_html)]."""
        terms, filters = parse_query(query)
        if not terms and not filters:
            return [], False
        with self._conn() as c:
            if not c.execute("SELECT 1 FROM chunks LIMIT 1").fetchone():
                return [], False
        results = self._search(terms, filters, False, limit)
        if results:
            return results, False
        if filters or any(kind == "word" for kind, _ in terms):   # nothing exact -> typos
            return self._search(terms, filters, True, limit), True
        return [], False

    def columns(self):
        """{file: [column headers]} for spreadsheets, so users know what filters exist."""
        with self._conn() as c:
            out = {}
            for f, h in c.execute(
                    "SELECT cols.file, cols.header FROM cols "
                    "JOIN files ON files.path=cols.file "
                    "WHERE files.size >= 0 ORDER BY cols.file, cols.rowid"):
                out.setdefault(f, []).append(h)
            return out