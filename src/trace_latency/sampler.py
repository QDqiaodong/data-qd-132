"""Offline tail-sampling collection pipeline for out-of-order trace JSONL.

``sample_traces(source, state_dir, output_dir, policy)`` consumes an
immutable UTF-8 JSONL telemetry stream and turns it into a recoverable
dataset of per-watermark sampling decisions:

* assembly: spans/finish records may arrive in any order; a trace is
  finalized at the first watermark ``W`` with ``finish.end <= W`` and is
  checked with the warehouse ``validate_sealed`` semantics (single root,
  complete references, reachable acyclic, interval containment) plus the
  declared unique span count, root id and root end tick;
* sampling: a valid trace is always kept when any span has ``error=true``
  or its root duration is ``>= slow_ticks``; every other trace competes
  for one of ``normal_budget`` slots per root service, ordered by the
  SHA256 digest bytes of the UTF-8 trace id, then the trace id bytes;
* recovery: all waiting state, duplicate evidence, the consumed byte
  offset and the last published watermark live in a SQLite state file;
  output directories are staged and renamed before the state transaction
  commits, so a kill at any of the three publication boundaries (files
  written / directory visible / state advanced) neither duplicates nor
  loses decisions on the next run.

Memory is bounded independently of the number of waiting traces and of
the candidate set size: the SQLite page caches are capped (see
``CACHE_KIB``), finalization streams traces in chunks, and budget
selection is an external sort inside the temporary store.  Only one
trace's spans (<= 5000 rows) are held in Python memory at a time.
"""
from pathlib import Path
import hashlib
import json
import os
import shutil
import sqlite3

from .common import require, ident, read as read_json, save as save_json
from .warehouse import validate_sealed, canonical_digest

MAX_LINE = 1_000_000          # one JSONL record may use at most 1 MB
MAX_TICK = 10**12
MAX_SPANS = 5000
MAX_BUDGET = 10000
BATCH_RECORDS = 2000          # input records per state transaction
FINALIZE_CHUNK = 500          # traces loaded per finalization query
CACHE_KIB = 8192              # 8 MiB per SQLite page cache (main + temp)

REASON_ERROR = 'error'
REASON_SLOW = 'slow'
REASON_NORMAL = 'normal'
REASON_BUDGET = 'budget'

# Stable quarantine codes (documented in README and docs/contract.md).
CODE_SPAN_COUNT = 'span_count_mismatch'
CODE_ROOT_COUNT = 'root_count'
CODE_UNKNOWN_PARENT = 'unknown_parent'
CODE_INTERVAL = 'interval_containment'
CODE_CYCLE = 'cycle'
CODE_ROOT_MISMATCH = 'root_mismatch'
CODE_ROOT_END = 'root_end_mismatch'
CODE_EMPTY = 'empty_trace'
CODE_INVALID = 'invalid_tree'

_SEAL_CODES = {
    'cannot seal an empty trace': CODE_EMPTY,
    'exactly one root required': CODE_ROOT_COUNT,
    'unknown parent': CODE_UNKNOWN_PARENT,
    'child outside parent interval': CODE_INTERVAL,
    'span cycle': CODE_CYCLE,
    'disconnected cycle': CODE_CYCLE,
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS traces (
    trace_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('waiting','finalized')),
    finish_root TEXT,
    finish_end INTEGER,
    finish_count INTEGER
);
CREATE INDEX IF NOT EXISTS idx_traces_finalize
    ON traces(finish_end)
    WHERE status='waiting' AND finish_end IS NOT NULL;
CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent TEXT,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    service TEXT NOT NULL,
    error INTEGER NOT NULL,
    PRIMARY KEY (trace_id, span_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS evidence (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    sha TEXT NOT NULL,
    PRIMARY KEY (trace_id, span_id)
) WITHOUT ROWID;
"""

TEMP_TABLES = """
CREATE TEMP TABLE candidates(
    service TEXT NOT NULL,
    digest BLOB NOT NULL,
    trace_id TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE TEMP TABLE quar(
    trace_id TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE TEMP TABLE evid(
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    sha TEXT NOT NULL
);
CREATE TEMP TABLE decisions(
    trace_id TEXT NOT NULL,
    service TEXT NOT NULL,
    reason TEXT NOT NULL
);
"""


# --------------------------------------------------------------------------- #
# strict JSONL record validation
# --------------------------------------------------------------------------- #
def _tick(value, name):
    # ``type(...) is int`` rejects ``bool`` as required.
    if type(value) is not int or not 0 <= value <= MAX_TICK:
        raise ValueError(name + ' must be an integer in 0..10^12 (bool rejected)')
    return value


def _count(value, name, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('%s must be an integer in %d..%d (bool rejected)'
                         % (name, minimum, maximum))
    return value


def _span(value):
    require(value, 'id parent start end service error')
    sid = ident(value['id'])
    parent = None if value['parent'] is None else ident(value['parent'])
    start = _tick(value['start'], 'start')
    end = _tick(value['end'], 'end')
    if end < start:
        raise ValueError('reversed span interval')
    service = ident(value['service'])
    error = value['error']
    if type(error) is not bool:
        raise ValueError('error must be a boolean')
    return {'id': sid, 'parent': parent, 'start': start, 'end': end,
            'service': service, 'error': error}


def _record(obj):
    """Validate one parsed JSONL object; return a normalised record tuple."""
    if not isinstance(obj, dict):
        raise ValueError('JSONL record must be an object')
    kind = obj.get('kind')
    if kind == 'span':
        require(obj, 'kind trace_id span')
        return ('span', ident(obj['trace_id']), _span(obj['span']))
    if kind == 'finish':
        require(obj, 'kind trace_id root_id end span_count')
        return ('finish', ident(obj['trace_id']),
                {'root_id': ident(obj['root_id']),
                 'end': _tick(obj['end'], 'end'),
                 'span_count': _count(obj['span_count'], 'span_count',
                                      1, MAX_SPANS)})
    if kind == 'watermark':
        require(obj, 'kind value')
        return ('watermark', _tick(obj['value'], 'value'))
    raise ValueError('record kind must be span, finish or watermark')


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key: ' + key)
        result[key] = value
    return result


def _nonfinite(value):
    raise ValueError('nonfinite JSON number: ' + value)


def _iter_jsonl(stream, offset):
    """Yield ``(offset_after_line, parsed_object)`` for each JSONL line.

    The final line may lack a newline; empty lines, lines over 1 MB,
    invalid UTF-8, duplicate keys and non-finite numbers are rejected.
    """
    while True:
        raw = stream.readline(MAX_LINE + 2)
        if raw == b'':
            return
        offset += len(raw)
        body = raw[:-1] if raw.endswith(b'\n') else raw
        if len(body) > MAX_LINE:
            raise ValueError('JSONL line exceeds 1 MB')
        if not body:
            raise ValueError('empty JSONL line')
        obj = json.loads(body.decode('utf-8'), object_pairs_hook=_unique,
                         parse_constant=_nonfinite)
        yield offset, obj


def read_policy(path):
    """Strictly validate ``{normal_budget, slow_ticks}``; return values+sha."""
    data, digest = read_json(path)
    require(data, 'normal_budget slow_ticks')
    budget = _count(data['normal_budget'], 'normal_budget', 0, MAX_BUDGET)
    slow = _count(data['slow_ticks'], 'slow_ticks', 0, MAX_TICK)
    return budget, slow, digest


def _sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------- #
# state connection
# --------------------------------------------------------------------------- #
def _connect(path):
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA cache_size=-%d' % CACHE_KIB)
    conn.execute('PRAGMA temp.cache_size=-%d' % CACHE_KIB)
    conn.execute('PRAGMA temp_store=FILE')
    conn.execute('PRAGMA busy_timeout=10000')
    return conn


def _ensure_schema(conn):
    conn.executescript(SCHEMA)


def _rollback_silent(conn):
    try:
        conn.execute('ROLLBACK')
    except sqlite3.Error:
        pass


def _set_meta(conn, key, value):
    conn.execute(
        'INSERT INTO meta(key,value) VALUES(?,?) '
        'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))


def _crash_hook(stage, seq):
    # Test-only fault injection: TRACE_LATENCY_SAMPLE_CRASH='stage:seq'
    # kills the process (no cleanup) right after one of the three
    # publication boundaries of the seq-th watermark of this run.
    spec = os.environ.get('TRACE_LATENCY_SAMPLE_CRASH')
    if spec and spec == '%s:%d' % (stage, seq):
        os._exit(1)


# --------------------------------------------------------------------------- #
# record ingestion
# --------------------------------------------------------------------------- #
def _process_span(conn, trace_id, span):
    row = conn.execute('SELECT status FROM traces WHERE trace_id=?',
                       (trace_id,)).fetchone()
    if row is None:
        conn.execute("INSERT INTO traces(trace_id,status) VALUES (?,'waiting')",
                     (trace_id,))
        status = 'waiting'
    else:
        status = row[0]
    sid = span['id']
    if status == 'waiting':
        existing = conn.execute(
            'SELECT parent,start,end,service,error FROM spans '
            'WHERE trace_id=? AND span_id=?', (trace_id, sid)).fetchone()
        if existing is not None:
            same = (existing[0] == span['parent']
                    and existing[1] == span['start']
                    and existing[2] == span['end']
                    and existing[3] == span['service']
                    and bool(existing[4]) == span['error'])
            if not same:
                raise ValueError('conflicting span record for trace ' + trace_id)
            return  # exact duplicate: ignore
        held = conn.execute('SELECT COUNT(*) FROM spans WHERE trace_id=?',
                            (trace_id,)).fetchone()[0]
        if held >= MAX_SPANS:
            raise ValueError('trace may hold at most %d spans' % MAX_SPANS)
        conn.execute(
            'INSERT INTO spans(trace_id,span_id,parent,start,end,service,error)'
            ' VALUES (?,?,?,?,?,?,?)',
            (trace_id, sid, span['parent'], span['start'], span['end'],
             span['service'], 1 if span['error'] else 0))
        return
    # Finalized trace: only exact duplicates of recorded evidence are allowed.
    sha = canonical_digest(span)
    stored = conn.execute('SELECT sha FROM evidence WHERE trace_id=? AND span_id=?',
                          (trace_id, sid)).fetchone()
    if stored is None:
        raise ValueError('new span for finalized trace ' + trace_id)
    if stored[0] != sha:
        raise ValueError('conflicting span for finalized trace ' + trace_id)


def _process_finish(conn, trace_id, finish, last_watermark):
    row = conn.execute(
        'SELECT status,finish_root,finish_end,finish_count FROM traces '
        'WHERE trace_id=?', (trace_id,)).fetchone()
    if row is None or (row[0] == 'waiting' and row[2] is None):
        # First finish of a trace must not retroactively enter an already
        # published watermark segment.
        if last_watermark is not None and finish['end'] <= last_watermark:
            raise ValueError('finish end does not exceed the published watermark')
        if row is None:
            conn.execute(
                "INSERT INTO traces(trace_id,status,finish_root,finish_end,"
                "finish_count) VALUES (?,'waiting',?,?,?)",
                (trace_id, finish['root_id'], finish['end'], finish['span_count']))
        else:
            conn.execute(
                'UPDATE traces SET finish_root=?,finish_end=?,finish_count=? '
                'WHERE trace_id=?',
                (finish['root_id'], finish['end'], finish['span_count'], trace_id))
        return
    declared = (row[1], row[2], row[3])
    actual = (finish['root_id'], finish['end'], finish['span_count'])
    if declared != actual:
        raise ValueError('conflicting finish record for trace ' + trace_id)
    # exact duplicate: ignore


# --------------------------------------------------------------------------- #
# finalization at one watermark
# --------------------------------------------------------------------------- #
def _validate_trace(spans, finish_root, finish_end, finish_count):
    """Return ``(quarantine_code, root)``; code is None for a valid trace."""
    if len(spans) != finish_count:
        return CODE_SPAN_COUNT, None
    try:
        _children, root = validate_sealed(spans)
    except ValueError as error:
        return _SEAL_CODES.get(str(error), CODE_INVALID), None
    if root != finish_root:
        return CODE_ROOT_MISMATCH, None
    if spans[root]['end'] != finish_end:
        return CODE_ROOT_END, None
    return None, root


def _collect_candidates(conn, watermark, slow_ticks):
    """Validate every trace whose finish end is covered by ``watermark``."""
    while True:
        rows = conn.execute(
            "SELECT trace_id,finish_root,finish_end,finish_count FROM traces "
            "WHERE status='waiting' AND finish_end IS NOT NULL AND finish_end<=? "
            "LIMIT ?", (watermark, FINALIZE_CHUNK)).fetchall()
        if not rows:
            return
        for trace_id, fin_root, fin_end, fin_count in rows:
            spans = {}
            for sid, parent, start, end, service, error in conn.execute(
                    'SELECT span_id,parent,start,end,service,error FROM spans '
                    'WHERE trace_id=?', (trace_id,)):
                spans[sid] = {'parent': parent, 'start': start, 'end': end,
                              'service': service, 'error': bool(error)}
            code, root = _validate_trace(spans, fin_root, fin_end, fin_count)
            if code is not None:
                conn.execute('INSERT INTO quar(trace_id,reason) VALUES(?,?)',
                             (trace_id, code))
            else:
                top = spans[root]
                duration = top['end'] - top['start']
                has_error = any(s['error'] for s in spans.values())
                if has_error:
                    reason = REASON_ERROR
                elif duration >= slow_ticks:
                    reason = REASON_SLOW
                else:
                    reason = REASON_NORMAL
                digest = hashlib.sha256(trace_id.encode('utf-8')).digest()
                conn.execute(
                    'INSERT INTO candidates(service,digest,trace_id,reason) '
                    'VALUES(?,?,?,?)', (top['service'], digest, trace_id, reason))
            # Flip the status inside the same transaction so the chunked
            # query above makes progress and never revisits this trace.
            conn.execute("UPDATE traces SET status='finalized' "
                         "WHERE trace_id=?", (trace_id,))
            # Duplicate-equality evidence is kept for every finalized trace;
            # past conflicts must stay detectable instead of being accepted.
            conn.executemany(
                'INSERT INTO evid(trace_id,span_id,sha) VALUES(?,?,?)',
                [(trace_id, sid, canonical_digest({'id': sid, **span}))
                 for sid, span in spans.items()])


def _select_budget(conn, budget):
    """Per-service normal-budget selection over the external-sorted stream."""
    current = None
    used = 0
    cursor = conn.execute(
        'SELECT service,digest,trace_id,reason FROM candidates '
        'ORDER BY service,digest,trace_id')
    for service, _digest, trace_id, reason in cursor:
        if service != current:
            current = service
            used = 0
        if reason == REASON_NORMAL:
            if used < budget:
                used += 1
                decision = REASON_NORMAL
            else:
                decision = REASON_BUDGET
        else:
            decision = reason
        conn.execute('INSERT INTO decisions(trace_id,service,reason) '
                     'VALUES(?,?,?)', (trace_id, service, decision))


def _line(obj):
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True,
                       separators=(',', ':'), allow_nan=False) + '\n'
            ).encode('utf-8')


def _write_outputs(conn, staging, watermark, source_sha, policy_sha):
    """Write kept/dropped/quarantine JSONL (trace_id byte order) + manifest."""
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    digests = {}
    kept = hashlib.sha256()
    dropped = hashlib.sha256()
    counts = {'kept.jsonl': 0, 'dropped.jsonl': 0, 'quarantine.jsonl': 0}
    kept_path = staging / 'kept.jsonl'
    dropped_path = staging / 'dropped.jsonl'
    with kept_path.open('wb') as kept_f, dropped_path.open('wb') as dropped_f:
        for trace_id, service, reason in conn.execute(
                'SELECT trace_id,service,reason FROM decisions '
                'ORDER BY trace_id'):
            if reason == REASON_BUDGET:
                line = _line({'trace_id': trace_id, 'service': service,
                              'reason': REASON_BUDGET})
                dropped_f.write(line)
                dropped.update(line)
                counts['dropped.jsonl'] += 1
            else:
                spans = [
                    {'id': sid, 'parent': parent, 'start': start, 'end': end,
                     'service': service_name, 'error': bool(error)}
                    for sid, parent, start, end, service_name, error
                    in conn.execute(
                        'SELECT span_id,parent,start,end,service,error '
                        'FROM spans WHERE trace_id=? ORDER BY span_id',
                        (trace_id,))]
                line = _line({'trace_id': trace_id, 'service': service,
                              'reason': reason, 'spans': spans})
                kept_f.write(line)
                kept.update(line)
                counts['kept.jsonl'] += 1
    quar = hashlib.sha256()
    with (staging / 'quarantine.jsonl').open('wb') as quar_f:
        for trace_id, reason in conn.execute(
                'SELECT trace_id,reason FROM quar ORDER BY trace_id'):
            line = _line({'trace_id': trace_id, 'reason': reason})
            quar_f.write(line)
            quar.update(line)
            counts['quarantine.jsonl'] += 1
    digests['kept.jsonl'] = kept.hexdigest()
    digests['dropped.jsonl'] = dropped.hexdigest()
    digests['quarantine.jsonl'] = quar.hexdigest()
    manifest = {
        'watermark': watermark,
        'source_sha256': source_sha,
        'policy_sha256': policy_sha,
        'files': {name: {'sha256': digests[name], 'count': counts[name]}
                  for name in ('kept.jsonl', 'dropped.jsonl',
                               'quarantine.jsonl')},
    }
    save_json(staging / 'manifest.json', manifest)
    return counts


def _files_equal(first, second):
    with open(first, 'rb') as one, open(second, 'rb') as two:
        while True:
            left = one.read(1 << 20)
            right = two.read(1 << 20)
            if left != right:
                return False
            if not left:
                return True


def _trees_equal(staging, dest):
    names = sorted(os.listdir(staging))
    if names != sorted(os.listdir(dest)):
        return False
    for name in names:
        here = os.path.join(staging, name)
        there = os.path.join(dest, name)
        if not os.path.isfile(here) or not os.path.isfile(there):
            return False
        if not _files_equal(here, there):
            return False
    return True


def _publish(staging, dest):
    """Make the staged watermark directory visible exactly once.

    A pre-existing ``dest`` belongs to a run killed between the rename and
    the state commit; it is verified byte-for-byte and never replaced.
    """
    if dest.exists():
        if not _trees_equal(str(staging), str(dest)):
            raise ValueError('output conflicts with already published %s'
                             % dest.name)
        shutil.rmtree(staging)
    else:
        os.rename(staging, dest)


def _sweep_state(conn):
    """Keep duplicate evidence for finalized traces, drop waiting spans."""
    finalized = ('SELECT trace_id FROM decisions '
                 'UNION SELECT trace_id FROM quar')
    conn.execute('INSERT INTO evidence(trace_id,span_id,sha) '
                 'SELECT trace_id,span_id,sha FROM evid')
    conn.execute('DELETE FROM spans WHERE trace_id IN (%s)' % finalized)


def _finalize_and_publish(conn, watermark, budget, slow_ticks, staging, dest,
                          source_sha, policy_sha, seq):
    """Finalize, sample and publish one watermark inside the open txn."""
    # No executescript here: it would implicitly COMMIT the open transaction.
    for statement in TEMP_TABLES.strip().split(';'):
        statement = statement.strip()
        if statement:
            conn.execute(statement)
    try:
        _collect_candidates(conn, watermark, slow_ticks)
        _select_budget(conn, budget)
        counts = _write_outputs(conn, staging, watermark, source_sha, policy_sha)
        _crash_hook('files', seq)
        _publish(staging, dest)
        _crash_hook('rename', seq)
        _sweep_state(conn)
        return counts
    finally:
        for name in ('candidates', 'quar', 'evid', 'decisions'):
            try:
                conn.execute('DROP TABLE IF EXISTS temp.%s' % name)
            except sqlite3.Error:
                pass  # never mask the original failure; temp tables die with the connection


# --------------------------------------------------------------------------- #
# main entry
# --------------------------------------------------------------------------- #
def sample_traces(source, state_dir, output_dir, policy):
    """Consume ``source`` JSONL into per-watermark sampling datasets.

    ``state_dir`` holds the resumable SQLite state, ``output_dir`` receives
    one ``watermark-W`` directory per published watermark and ``policy`` is
    the strict ``{normal_budget, slow_ticks}`` JSON file.  Returns the run
    report; raises ``ValueError``/``OSError`` (exit 2 at the CLI) on any
    contract, identity or conflict violation.
    """
    source = Path(source)
    state_dir = Path(state_dir)
    output_dir = Path(output_dir)
    budget, slow_ticks, policy_sha = read_policy(policy)
    source_sha = _sha256_file(source)
    state_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_file = state_dir / 'sampler.db'
    # A bound sampler locates and verifies its catalog itself; an active
    # retirement request closes this entry before any business data is read
    # and before consumption offsets or watermarks can advance.
    from .retirement import gate_for_db
    gate = gate_for_db(db_file, 'sampler_db')
    conn = _connect(db_file)
    staging = None
    try:
        _ensure_schema(conn)
        stored = dict(conn.execute('SELECT key,value FROM meta').fetchall())
        if 'source_sha256' in stored:
            if stored['source_sha256'] != source_sha:
                raise ValueError('source file identity changed')
            if stored['policy_sha256'] != policy_sha:
                raise ValueError('policy identity changed')
        else:
            conn.execute('BEGIN IMMEDIATE')
            _set_meta(conn, 'source_sha256', source_sha)
            _set_meta(conn, 'policy_sha256', policy_sha)
            _set_meta(conn, 'offset', '0')
            conn.execute('COMMIT')
        # Leftover staging directories of a killed run were never visible.
        for entry in output_dir.iterdir():
            if entry.name.startswith('.staging-') and entry.is_dir():
                shutil.rmtree(entry)
        offset = int(stored.get('offset', '0'))
        last_watermark = stored.get('last_watermark')
        last_watermark = None if last_watermark is None else int(last_watermark)
        stats = {'watermarks': 0, 'kept': 0, 'dropped': 0, 'quarantined': 0}
        pending_records = 0
        conn.execute('BEGIN IMMEDIATE')
        with source.open('rb') as stream:
            stream.seek(offset)
            for offset, obj in _iter_jsonl(stream, offset):
                record = _record(obj)
                kind = record[0]
                # Retired ids are skipped on sight: structurally valid
                # span/finish records (including content that differs from
                # anything seen before) create no trace, spend no budget and
                # never trigger past-conflict errors; the offset still
                # advances, so normal consumption proceeds.
                if gate is not None and kind in ('span', 'finish') \
                        and gate.is_blocked(record[1]):
                    pending_records += 1
                    if pending_records >= BATCH_RECORDS:
                        _set_meta(conn, 'offset', str(offset))
                        conn.execute('COMMIT')
                        conn.execute('BEGIN IMMEDIATE')
                        pending_records = 0
                    continue
                if kind == 'span':
                    _process_span(conn, record[1], record[2])
                elif kind == 'finish':
                    _process_finish(conn, record[1], record[2], last_watermark)
                else:
                    value = record[1]
                    if last_watermark is not None and value <= last_watermark:
                        raise ValueError('watermark value must strictly increase')
                    staging = output_dir / ('.staging-%d' % value)
                    counts = _finalize_and_publish(
                        conn, value, budget, slow_ticks, staging,
                        output_dir / ('watermark-%d' % value),
                        source_sha, policy_sha, stats['watermarks'] + 1)
                    staging = None
                    last_watermark = value
                    _set_meta(conn, 'offset', str(offset))
                    _set_meta(conn, 'last_watermark', str(value))
                    conn.execute('COMMIT')
                    _crash_hook('commit', stats['watermarks'] + 1)
                    stats['watermarks'] += 1
                    stats['kept'] += counts['kept.jsonl']
                    stats['dropped'] += counts['dropped.jsonl']
                    stats['quarantined'] += counts['quarantine.jsonl']
                    conn.execute('BEGIN IMMEDIATE')
                    pending_records = 0
                    continue
                pending_records += 1
                if pending_records >= BATCH_RECORDS:
                    _set_meta(conn, 'offset', str(offset))
                    conn.execute('COMMIT')
                    conn.execute('BEGIN IMMEDIATE')
                    pending_records = 0
            _set_meta(conn, 'offset', str(offset))
            conn.execute('COMMIT')
        pending = conn.execute(
            "SELECT COUNT(*) FROM traces WHERE status='waiting'").fetchone()[0]
        return {'schema_version': 3, 'kind': 'sample_traces',
                'source_sha256': source_sha, 'policy_sha256': policy_sha,
                'watermarks_published': stats['watermarks'],
                'last_watermark': last_watermark, 'pending': pending,
                'kept': stats['kept'], 'dropped': stats['dropped'],
                'quarantined': stats['quarantined']}
    except BaseException:
        _rollback_silent(conn)
        if staging is not None:
            shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        conn.close()
        if gate is not None:
            gate.close()
