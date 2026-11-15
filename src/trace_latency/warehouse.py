"""Offline SQLite warehouse for revocable service-occupancy analysis.

A warehouse stores revision batches for traces and maintains a persistent
service/time concurrency index for sealed traces only.  All writes for one
batch happen inside a single ``BEGIN IMMEDIATE`` transaction so span state,
per-trace contributions, the Fenwick prefix aggregate, ``max_seq`` and the
idempotency row become visible atomically.

Index layout (see README for the full complexity argument):

* ``contrib_endpoints`` holds one +1/-1 row per sealed trace contribution
  endpoint, deletable per trace and range-scannable per ``(service, pos)``.
* ``bit_index`` is a sparse Fenwick tree (1-based node ``j = pos + 1`` over a
  domain of ``U = 10**12 + 1`` nodes).  A prefix base count needs at most
  ``ceil(log2 U) <= 40`` primary-key lookups.
"""
from pathlib import Path
import hashlib, json, sqlite3

from .common import require, ident

MAX_TICKS = 10**12
DOMAIN = MAX_TICKS + 1          # Fenwick domain size U
MAX_EVENTS = 5000
MAX_SPANS = 5000
OPS = frozenset(('upsert', 'delete', 'seal'))
LOG_U = 40                      # ceil(log2(10**12+1)), Fenwick hop bound

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    seq_from INTEGER NOT NULL,
    seq_to INTEGER NOT NULL,
    content_sha TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS traces (
    trace_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('draft','sealed')),
    version INTEGER NOT NULL,
    update_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent TEXT,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    service TEXT NOT NULL,
    PRIMARY KEY (trace_id, span_id)
);
CREATE TABLE IF NOT EXISTS contrib_endpoints (
    service TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    pos INTEGER NOT NULL,
    delta INTEGER NOT NULL,
    PRIMARY KEY (service, trace_id, pos, delta)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_contrib_service_pos
    ON contrib_endpoints(service, pos);
CREATE INDEX IF NOT EXISTS idx_contrib_trace
    ON contrib_endpoints(trace_id);
CREATE TABLE IF NOT EXISTS bit_index (
    service TEXT NOT NULL,
    node INTEGER NOT NULL,
    total INTEGER NOT NULL,
    PRIMARY KEY (service, node)
) WITHOUT ROWID;
"""


# --------------------------------------------------------------------------- #
# validation
# --------------------------------------------------------------------------- #
def _tick(value, name):
    # ``type(...) is int`` rejects ``bool`` as required.
    if type(value) is not int or not 0 <= value <= MAX_TICKS:
        raise ValueError(name + ' must be an integer in 0..10^12 (bool rejected)')
    return value


def _seq(value):
    if type(value) is not int or value < 1:
        raise ValueError('seq must be a positive integer (bool rejected)')
    return value


def _span(value):
    require(value, 'id parent start end service')
    sid = ident(value['id'])
    parent = None if value['parent'] is None else ident(value['parent'])
    start = _tick(value['start'], 'start')
    end = _tick(value['end'], 'end')
    if end < start:
        raise ValueError('reversed span interval')
    service = ident(value['service'])
    return {'id': sid, 'parent': parent, 'start': start, 'end': end,
            'service': service}


def _event(value, index):
    if not isinstance(value, dict):
        raise ValueError('event %d must be a JSON object' % index)
    keys = set(value)
    if not {'seq', 'trace_id', 'op'} <= keys:
        raise ValueError('event requires seq, trace_id and op')
    seq = _seq(value['seq'])
    trace_id = ident(value['trace_id'])
    op = value['op']
    if not isinstance(op, str) or op not in OPS:
        raise ValueError('op must be upsert, delete or seal')
    if op == 'upsert':
        if keys != {'seq', 'trace_id', 'op', 'span'}:
            raise ValueError('upsert event allows exactly seq, trace_id, op, span')
        return ('upsert', seq, trace_id, _span(value['span']))
    if op == 'delete':
        if keys != {'seq', 'trace_id', 'op', 'id'}:
            raise ValueError('delete event allows exactly seq, trace_id, op, id')
        return ('delete', seq, trace_id, ident(value['id']))
    if keys != {'seq', 'trace_id', 'op'}:
        raise ValueError('seal event allows exactly seq, trace_id, op')
    return ('seal', seq, trace_id, None)


def validate_batch(data):
    """Strictly validate ``{batch_id, events}``; return (batch_id, events)."""
    require(data, 'batch_id events')
    batch_id = ident(data['batch_id'])
    events = data['events']
    if not isinstance(events, list) or not events:
        raise ValueError('events must be a non-empty array')
    if len(events) > MAX_EVENTS:
        raise ValueError('at most 5000 events per batch')
    return batch_id, [_event(e, i) for i, e in enumerate(events)]


def validate_query(data):
    require(data, 'service start end')
    service = ident(data['service'])
    start = _tick(data['start'], 'start')
    end = _tick(data['end'], 'end')
    if not start < end:
        raise ValueError('query requires start < end')
    return service, start, end


def canonical_digest(data):
    """SHA256 over canonical JSON so object key order cannot change equality."""
    raw = json.dumps(data, sort_keys=True, separators=(',', ':'),
                     ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


# --------------------------------------------------------------------------- #
# connection / schema
# --------------------------------------------------------------------------- #
def connect(db_path):
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=10000')
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


def ensure_schema(conn):
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('max_seq','0')")
    conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('warehouse_version','1')")


def _rollback_silent(conn):
    try:
        conn.execute('ROLLBACK')
    except sqlite3.Error:
        pass


def _get_max_seq(conn):
    row = conn.execute("SELECT value FROM meta WHERE key='max_seq'").fetchone()
    return int(row[0]) if row else 0


# --------------------------------------------------------------------------- #
# Fenwick (BIT) prefix aggregate
# --------------------------------------------------------------------------- #
def bit_add(conn, service, pos, delta, touched):
    node = pos + 1
    while node <= DOMAIN:
        conn.execute(
            'INSERT INTO bit_index(service,node,total) VALUES(?,?,?) '
            'ON CONFLICT(service,node) DO UPDATE SET total=bit_index.total+?',
            (service, node, delta, delta))
        touched.setdefault(service, set()).add(node)
        node += node & -node


def bit_prefix(conn, service, j):
    """Sum deltas at Fenwick indices 1..j (positions 0..j-1)."""
    total = 0
    while j > 0:
        row = conn.execute(
            'SELECT total FROM bit_index WHERE service=? AND node=?',
            (service, j)).fetchone()
        if row is not None:
            total += row[0]
        j -= j & -j
    return total


def _cleanup_zero_nodes(conn, touched):
    for service, nodes in touched.items():
        nodes = list(nodes)
        for i in range(0, len(nodes), 900):
            chunk = nodes[i:i + 900]
            marks = ','.join('?' * len(chunk))
            conn.execute(
                'DELETE FROM bit_index WHERE service=? AND total=0 AND node IN (%s)' % marks,
                [service] + chunk)


# --------------------------------------------------------------------------- #
# fragment geometry
# --------------------------------------------------------------------------- #
def fragments(start, end, child_intervals):
    """[start,end) minus the union of contained direct-child intervals."""
    out = []
    cursor = start
    for a, b in sorted(child_intervals):
        if b <= cursor:
            continue
        a = max(a, cursor)
        if a > cursor:
            out.append((cursor, a))
        cursor = b
    if cursor < end:
        out.append((cursor, end))
    return [(a, b) for a, b in out if b > a]


def union_intervals(intervals):
    """Merge overlapping or touching half-open intervals; drop zero length."""
    merged = []
    for a, b in sorted(intervals):
        if b <= a:
            continue
        if not merged or a > merged[-1][1]:
            merged.append([a, b])
        elif b > merged[-1][1]:
            merged[-1][1] = b
    return [(a, b) for a, b in merged if b > a]


def validate_sealed(spans):
    """Single root, complete refs, reachable acyclic, interval containment."""
    if not spans:
        raise ValueError('cannot seal an empty trace')
    roots = [sid for sid, s in spans.items() if s['parent'] is None]
    if len(roots) != 1:
        raise ValueError('exactly one root required')
    children = {sid: [] for sid in spans}
    for sid, s in spans.items():
        parent = s['parent']
        if parent is None:
            continue
        if parent not in spans:
            raise ValueError('unknown parent')
        if not (spans[parent]['start'] <= s['start'] <= s['end']
                <= spans[parent]['end']):
            raise ValueError('child outside parent interval')
        children[parent].append(sid)
    seen = set()
    pending = [roots[0]]
    while pending:
        sid = pending.pop()
        if sid in seen:
            raise ValueError('span cycle')
        seen.add(sid)
        pending.extend(children[sid])
    if len(seen) != len(spans):
        raise ValueError('disconnected cycle')
    return children, roots[0]


def trace_contributions(spans):
    """Return (service, pos, delta) endpoint rows for one validated trace."""
    children, _root = validate_sealed(spans)
    by_service = {}
    for sid, s in spans.items():
        child_intervals = [(spans[c]['start'], spans[c]['end'])
                           for c in children[sid]]
        for a, b in fragments(s['start'], s['end'], child_intervals):
            by_service.setdefault(s['service'], []).append((a, b))
    rows = []
    for service, intervals in by_service.items():
        for a, b in union_intervals(intervals):
            rows.append((service, a, 1))
            rows.append((service, b, -1))
    return rows


# --------------------------------------------------------------------------- #
# batch application
# --------------------------------------------------------------------------- #
def _load_state(conn, cache, trace_id):
    if trace_id in cache:
        return cache[trace_id]
    row = conn.execute(
        'SELECT status,version,update_seq FROM traces WHERE trace_id=?',
        (trace_id,)).fetchone()
    if row is None:
        state = {'status': None, 'version': 0, 'update_seq': 0,
                 'spans': {}, 'touched': False}
    else:
        state = {'status': row[0], 'version': row[1], 'update_seq': row[2],
                 'spans': {}, 'touched': False}
        for sid, parent, start, end, service in conn.execute(
                'SELECT span_id,parent,start,end,service FROM spans WHERE trace_id=?',
                (trace_id,)):
            state['spans'][sid] = {'id': sid, 'parent': parent, 'start': start,
                                   'end': end, 'service': service}
    cache[trace_id] = state
    return state


def _revoke_trace(conn, trace_id, touched):
    rows = conn.execute(
        'SELECT service,pos,delta FROM contrib_endpoints WHERE trace_id=?',
        (trace_id,)).fetchall()
    for service, pos, delta in rows:
        bit_add(conn, service, pos, -delta, touched)
    conn.execute('DELETE FROM contrib_endpoints WHERE trace_id=?', (trace_id,))


def _touch(conn, state, trace_id, seq, touched):
    if state['status'] is None:
        state['status'] = 'draft'
        state['version'] = 1
        state['touched'] = True
        conn.execute(
            'INSERT INTO traces(trace_id,status,version,update_seq) VALUES(?,?,1,?)',
            (trace_id, 'draft', seq))
        return
    if state['status'] == 'sealed':
        # Any upsert/delete on a sealed trace reopens it: the whole-trace
        # contribution is revoked inside the same transaction and it becomes
        # a draft again, even when the earlier seal happened in this batch.
        _revoke_trace(conn, trace_id, touched)
        state['status'] = 'draft'
        state['version'] += 1
        state['touched'] = True
        conn.execute(
            'UPDATE traces SET status=?,version=?,update_seq=? WHERE trace_id=?',
            ('draft', state['version'], seq, trace_id))
    elif not state['touched']:
        # First revision of an already-draft trace within this batch.
        state['version'] += 1
        state['touched'] = True
        conn.execute(
            'UPDATE traces SET version=?,update_seq=? WHERE trace_id=?',
            (state['version'], seq, trace_id))
    else:
        conn.execute(
            'UPDATE traces SET update_seq=? WHERE trace_id=?', (seq, trace_id))


def _do_upsert(conn, state, trace_id, span, seq, touched):
    _touch(conn, state, trace_id, seq, touched)
    sid = span['id']
    if sid not in state['spans'] and len(state['spans']) >= MAX_SPANS:
        raise ValueError('trace may hold at most 5000 spans')
    state['spans'][sid] = span
    conn.execute(
        'INSERT INTO spans(trace_id,span_id,parent,start,end,service) '
        'VALUES(?,?,?,?,?,?) ON CONFLICT(trace_id,span_id) DO UPDATE SET '
        'parent=excluded.parent,start=excluded.start,end=excluded.end,'
        'service=excluded.service',
        (trace_id, sid, span['parent'], span['start'], span['end'],
         span['service']))


def _do_delete(conn, state, trace_id, sid, seq, touched):
    if state['status'] is None or sid not in state['spans']:
        raise ValueError('delete target span does not exist')
    _touch(conn, state, trace_id, seq, touched)
    del state['spans'][sid]
    conn.execute('DELETE FROM spans WHERE trace_id=? AND span_id=?',
                 (trace_id, sid))


def _do_seal(conn, state, trace_id, seq, touched):
    if state['status'] is None:
        raise ValueError('cannot seal a trace without spans')
    if state['status'] == 'sealed':
        raise ValueError('trace is already sealed')
    rows = trace_contributions(state['spans'])
    conn.executemany(
        'INSERT INTO contrib_endpoints(service,trace_id,pos,delta) VALUES(?,?,?,?)',
        [(service, trace_id, pos, delta) for service, pos, delta in rows])
    for service, pos, delta in rows:
        bit_add(conn, service, pos, delta, touched)
    state['status'] = 'sealed'
    conn.execute(
        "UPDATE traces SET status='sealed',update_seq=? WHERE trace_id=?",
        (seq, trace_id))


def apply_locked(conn, batch_id, events, digest, touched=None):
    """Apply an already validated batch on a connection holding a write tx."""
    if touched is None:
        touched = {}
    max_seq = _get_max_seq(conn)
    for offset, (kind, seq, _trace_id, _payload) in enumerate(events):
        if seq != max_seq + 1 + offset:
            raise ValueError('seq must continue the global 1..N sequence')
    cache = {}
    for kind, seq, trace_id, payload in events:
        state = _load_state(conn, cache, trace_id)
        if kind == 'upsert':
            _do_upsert(conn, state, trace_id, payload, seq, touched)
        elif kind == 'delete':
            _do_delete(conn, state, trace_id, payload, seq, touched)
        else:
            _do_seal(conn, state, trace_id, seq, touched)
    new_max = max_seq + len(events)
    conn.execute("UPDATE meta SET value=? WHERE key='max_seq'", (str(new_max),))
    conn.execute(
        'INSERT INTO batches(batch_id,seq_from,seq_to,content_sha) VALUES(?,?,?,?)',
        (batch_id, max_seq + 1, new_max, digest))
    _cleanup_zero_nodes(conn, touched)
    return {'batch_id': batch_id, 'seq_from': max_seq + 1, 'seq_to': new_max,
            'events': len(events), 'replayed': False, 'max_seq': new_max}


def apply_batch(db_path, data):
    """Validate and atomically commit one batch; idempotent on batch_id."""
    batch_id, events = validate_batch(data)
    digest = canonical_digest(data)
    conn = connect(db_path)
    try:
        ensure_schema(conn)
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute(
            'SELECT seq_from,seq_to,content_sha FROM batches WHERE batch_id=?',
            (batch_id,)).fetchone()
        if row is not None:
            if row[2] != digest:
                raise ValueError('batch_id was already committed with different content')
            conn.execute('COMMIT')
            return {'batch_id': batch_id, 'seq_from': row[0], 'seq_to': row[1],
                    'events': len(events), 'replayed': True,
                    'max_seq': _get_max_seq(conn)}
        result = apply_locked(conn, batch_id, events, digest)
        conn.execute('COMMIT')
        return result
    except BaseException:
        _rollback_silent(conn)
        raise
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# queries
# --------------------------------------------------------------------------- #
def _zero_result(service, start, end, max_seq=0):
    return {'service': service, 'start': start, 'end': end, 'max_seq': max_seq,
            'histogram': [{'concurrency': 0, 'ticks': end - start}],
            'peak': 0, 'occupied_ticks': 0, 'trace_ticks': 0}


def query(db_path, data):
    """Consistent-snapshot occupancy histogram for one service/window."""
    service, start, end = validate_query(data)
    path = Path(db_path)
    if not path.exists():
        return _zero_result(service, start, end)
    conn = connect(path)
    try:
        is_warehouse = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'").fetchone()
        if is_warehouse is None:
            raise ValueError('database is not a trace-latency warehouse')
        conn.execute('BEGIN')
        try:
            max_seq = _get_max_seq(conn)
            # Left-side base count: <= LOG_U primary-key lookups, never
            # assuming the concurrency at the window start is zero.
            concurrency = bit_prefix(conn, service, start)
            peak = concurrency
            buckets = {}
            cursor = start
            # Only in-window endpoint rows of this service are read.
            groups = conn.execute(
                'SELECT pos,SUM(delta) FROM contrib_endpoints '
                'WHERE service=? AND pos>=? AND pos<? GROUP BY pos ORDER BY pos',
                (service, start, end)).fetchall()
            for pos, delta in groups:
                if pos > cursor:
                    buckets[concurrency] = buckets.get(concurrency, 0) + (pos - cursor)
                concurrency += delta
                if concurrency > peak:
                    peak = concurrency
                cursor = pos
            if end > cursor:
                buckets[concurrency] = buckets.get(concurrency, 0) + (end - cursor)
            if concurrency > peak:
                peak = concurrency
            histogram = [{'concurrency': level, 'ticks': ticks}
                         for level, ticks in sorted(buckets.items()) if ticks > 0]
            occupied = sum(ticks for level, ticks in buckets.items() if level > 0)
            trace_ticks = sum(level * ticks for level, ticks in buckets.items())
            result = {'service': service, 'start': start, 'end': end,
                      'max_seq': max_seq, 'histogram': histogram, 'peak': peak,
                      'occupied_ticks': occupied, 'trace_ticks': trace_ticks}
            conn.execute('COMMIT')
            return result
        except BaseException:
            _rollback_silent(conn)
            raise
    finally:
        conn.close()
