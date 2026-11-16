"""Targeted offline retirement of trace payloads.

A retirement catalog binds exactly one managed set of three resources:

* a warehouse SQLite database (``warehouse.apply_batch`` / ``query``),
* a sampler state directory containing ``sampler.db``,
* the sampler output root holding the published ``watermark-W`` trees.

The catalog directory owns the durable retirement control database
(``retire.db``) plus per-request staging space.  Managed resources locate
their catalog through a ``retirement_catalog`` meta row (the two SQLite
files) or a marker file (the output root), so no entry point needs extra
parameters to find the control plane: omitting ``retire-traces`` flags can
never bypass a gate.

Honesty model across the non-atomic resources:

1. A request is durably accepted (request id, canonical set digest,
   monotonic sequence, active status, cumulative blocked set) before any
   payload is touched.
2. Every destructive step first persists its *first-run* counts in a
   ``progress`` row and only then commits/deletes, so a kill after a
   transaction commit or a single file replacement cannot make a retry
   under-report the originally observed counts.
3. The request stays ``active`` -- and every managed entry stays closed --
   until every step is finished; the completion marker is the very last
   durable action.

Only the Python standard library is used.  Memory stays bounded: the
request set holds at most 10 000 ids, every scan streams rows/lines, and
only one trace (at most 5000 spans) or one JSONL line is materialised at
a time.
"""
from pathlib import Path
import hashlib
import json
import os
import re
import sqlite3
import tempfile

from .common import require, ident, read as read_json, save as save_json
from .warehouse import (
    connect as warehouse_connect,
    ensure_schema as warehouse_ensure_schema,
    _revoke_trace,
    _cleanup_zero_nodes,
    canonical_digest,
)

MAX_REQUEST_IDS = 10_000
DELETE_CHUNK = 500
WATERMARK_RE = re.compile(r'watermark-(\d+)\Z')
OUTPUT_FILES = ('kept.jsonl', 'dropped.jsonl', 'quarantine.jsonl')
CATALOG_META_KEY = 'retirement_catalog'
OUTPUT_MARKER = '.trace-latency-retirement'
CONTROL_DB = 'retire.db'
SCHEMA_VERSION = 1

CONTROL_SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL UNIQUE,
    set_sha256 TEXT NOT NULL,
    trace_count INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active','done')),
    report_json TEXT
);
CREATE TABLE IF NOT EXISTS blocked (
    trace_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS progress (
    request_id TEXT NOT NULL,
    step TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('prepared','done')),
    detail_json TEXT NOT NULL,
    PRIMARY KEY (request_id, step)
);
"""


class RetirementError(ValueError):
    """A retirement contract, binding or gate violation (CLI exit 2)."""


# --------------------------------------------------------------------------- #
# test-only fault injection
# --------------------------------------------------------------------------- #
def _crash(stage, suffix=None):
    """Kill the process without cleanup at a named maintenance boundary.

    ``TRACE_LATENCY_RETIRE_CRASH`` is test-only; its value matches either
    a bare stage (``accept``, ``warehouse_commit``, ``sampler_commit``,
    ``complete``) or ``stage:suffix`` for per-watermark points
    (``wmbefore:10``, ``wmafterfiles:10``, ``wmdone:10``).
    """
    spec = os.environ.get('TRACE_LATENCY_RETIRE_CRASH')
    if not spec:
        return
    wanted = stage if suffix is None else '%s:%s' % (stage, suffix)
    if spec == wanted:
        os._exit(1)


# --------------------------------------------------------------------------- #
# request validation
# --------------------------------------------------------------------------- #
def validate_request(data):
    """Strictly validate ``{request_id, trace_ids}``; return (id, sorted ids).

    The id list must be non-empty, hold at most 10 000 distinct identifiers
    passing the shared ``ident`` rules, and is normalised by UTF-8 byte
    order so retries are order-independent.
    """
    require(data, 'request_id trace_ids')
    request_id = ident(data['request_id'])
    raw = data['trace_ids']
    if not isinstance(raw, list) or not raw:
        raise RetirementError('trace_ids must be a non-empty array')
    if len(raw) > MAX_REQUEST_IDS:
        raise RetirementError('at most %d trace_ids per request' % MAX_REQUEST_IDS)
    seen = set()
    for value in raw:
        tid = ident(value)
        if tid in seen:
            raise RetirementError('trace_ids must be distinct')
        seen.add(tid)
    ordered = sorted(seen, key=lambda tid: tid.encode('utf-8'))
    return request_id, ordered


def request_set_digest(ordered_ids):
    return hashlib.sha256(canonical_digest(ordered_ids).encode('ascii')).hexdigest()


# --------------------------------------------------------------------------- #
# small filesystem / sqlite helpers
# --------------------------------------------------------------------------- #
def _canonical(path):
    return str(Path(path).expanduser().resolve(strict=False))


def _sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _write_text_atomic(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix='.tmp-')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(text)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def _connect_control(path):
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=10000')
    conn.execute('PRAGMA cache_size=-8192')
    conn.execute('PRAGMA temp_store=FILE')
    return conn


def _read_meta(conn, key):
    row = conn.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return None if row is None else row[0]


def _set_meta(conn, key, value):
    conn.execute(
        'INSERT INTO meta(key,value) VALUES(?,?) '
        'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))


def _rollback_silent(conn):
    try:
        conn.execute('ROLLBACK')
    except sqlite3.Error:
        pass


# --------------------------------------------------------------------------- #
# catalog initialization and binding
# --------------------------------------------------------------------------- #
def _verify_warehouse_resource(path):
    if not Path(path).is_file():
        raise RetirementError('warehouse db does not exist: ' + path)
    conn = warehouse_connect(path)
    try:
        warehouse_ensure_schema(conn)
        if conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
        ).fetchone() is None:
            raise RetirementError('not a trace-latency warehouse: ' + path)
        if _read_meta(conn, 'warehouse_version') is None:
            raise RetirementError('not a trace-latency warehouse: ' + path)
        # SLO pipeline and the retirement catalog are mutually exclusive; the
        # check happens before any binding or cleanup-style write.
        has_slo = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='slo_meta'"
        ).fetchone() is not None
        if has_slo and conn.execute(
                "SELECT 1 FROM slo_meta WHERE key='enabled'").fetchone() is not None:
            raise RetirementError(
                'warehouse has the SLO pipeline enabled; retirement catalog is mutually exclusive')
        return _read_meta(conn, CATALOG_META_KEY)
    finally:
        conn.close()


def _verify_sampler_resource(path):
    if not Path(path).is_file():
        raise RetirementError('sampler db does not exist: ' + path)
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        if conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
        ).fetchone() is None:
            raise RetirementError('not a sampler state database: ' + path)
        keys = {row[0] for row in conn.execute('SELECT key FROM meta')}
        if not {'source_sha256', 'policy_sha256'} <= keys:
            raise RetirementError('sampler db lacks source/policy identity: ' + path)
        return _read_meta(conn, CATALOG_META_KEY)
    finally:
        conn.close()


def _verify_output_tree(output_root, last_watermark, inflight=None):
    """Validate every published tree; return [(watermark, manifest_sha)].

    ``inflight`` maps a watermark value to its durable prepared cleanup detail
    (from the control ``progress`` table).  For an in-flight watermark the
    data files may be a mix of original and already-replaced bytes after a
    kill between the replacements, so each file digest/count must match
    either the published manifest or the durable target -- never anything
    else.  All other trees are verified strictly.
    """
    inflight = {} if inflight is None else inflight
    watermarks = []
    for entry in sorted(output_root.iterdir(), key=lambda p: p.name):
        if entry.name == OUTPUT_MARKER:
            if not entry.is_file():
                raise RetirementError('catalog marker is not a file')
            continue
        match = WATERMARK_RE.fullmatch(entry.name)
        if match is None:
            raise RetirementError(
                'output root may only hold watermark directories: ' + entry.name)
        if not entry.is_dir():
            raise RetirementError('watermark entry is not a directory: ' + entry.name)
        names = sorted(p.name for p in entry.iterdir())
        if names != sorted(OUTPUT_FILES + ('manifest.json',)):
            raise RetirementError('watermark directory has unexpected files: '
                                  + entry.name)
        manifest_path = entry / 'manifest.json'
        try:
            manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError) as error:
            raise RetirementError('unreadable manifest in ' + entry.name
                                  + ': ' + str(error))
        value = int(match.group(1))
        if manifest.get('watermark') != value:
            raise RetirementError('manifest watermark mismatch: ' + entry.name)
        files = manifest.get('files')
        if not isinstance(files, dict) or set(files) != set(OUTPUT_FILES):
            raise RetirementError('manifest files summary invalid: ' + entry.name)
        target = inflight.get(value)
        for name in OUTPUT_FILES:
            info = files[name]
            if not isinstance(info, dict) or 'sha256' not in info or 'count' not in info:
                raise RetirementError('manifest summary invalid: ' + entry.name)
            data_path = entry / name
            actual_sha = _sha256_file(data_path)
            count = 0
            with data_path.open('rb') as stream:
                for line in stream:
                    if not line.endswith(b'\n'):
                        raise RetirementError('JSONL line missing newline: '
                                              + str(data_path))
                    count += 1
            if target is not None and name in target['files']:
                wanted = target['files'][name]
                if actual_sha == wanted['sha256']:
                    if count != wanted['count']:
                        raise RetirementError(
                            'in-flight file count mismatch: ' + str(data_path))
                elif actual_sha != info['sha256'] or count != info['count']:
                    raise RetirementError(
                        'in-flight file matches neither target nor manifest: '
                        + str(data_path))
            else:
                if actual_sha != info['sha256']:
                    raise RetirementError(
                        'file digest mismatch: ' + str(data_path))
                if count != info['count']:
                    raise RetirementError(
                        'file count mismatch: ' + str(data_path))
        watermarks.append((value, _sha256_file(manifest_path)))
    published = {w for w, _ in watermarks}
    if last_watermark is None:
        if watermarks:
            raise RetirementError(
                'sampler has no published watermark but output root is not empty')
    else:
        if published and max(published) != last_watermark:
            raise RetirementError(
                'output high-water mark does not match sampler last_watermark')
    return watermarks


def _read_inflight(catalog_dir):
    """Load durable prepared watermark details for tolerant re-verification."""
    control = Path(catalog_dir) / CONTROL_DB
    if not control.is_file():
        return {}
    conn = _connect_control(control)
    try:
        rows = conn.execute(
            "SELECT step,detail_json FROM progress "
            "WHERE status='prepared' AND step LIKE 'output-%'").fetchall()
    except sqlite3.Error:
        return {}
    finally:
        conn.close()
    inflight = {}
    for step, detail_json in rows:
        try:
            detail = json.loads(detail_json)
            inflight[int(detail['watermark'])] = detail
        except (ValueError, KeyError):
            continue
    return inflight


def _validate_paths(catalog_dir, warehouse_db, sampler_db, output_root,
                    request_source):
    catalog_dir = Path(_canonical(catalog_dir))
    warehouse_db = _canonical(warehouse_db)
    sampler_db = _canonical(sampler_db)
    output_root = _canonical(output_root)
    request_source = _canonical(request_source)
    paths = (warehouse_db, sampler_db, output_root)
    if len(set(paths)) != 3:
        raise RetirementError('the three managed resources must be distinct')
    for target in paths:
        # Neither the read-only request source nor the catalog itself may be
        # used as a managed target, and catalog/target nesting would let one
        # resource's cleanup delete another resource's storage.
        if target == request_source:
            raise RetirementError('the request source cannot be a managed target')
        if target == str(catalog_dir) or target.startswith(
                str(catalog_dir) + os.sep):
            raise RetirementError('a managed target cannot live inside the catalog')
        if str(catalog_dir).startswith(target + os.sep):
            raise RetirementError('the catalog cannot live inside a managed target')
    # The sampler state directory and the output root must not overlap.
    state_dir = str(Path(sampler_db).parent)
    if output_root == state_dir or output_root.startswith(state_dir + os.sep) \
            or state_dir.startswith(output_root + os.sep):
        raise RetirementError('sampler state dir and output root must not overlap')
    if warehouse_db == state_dir or warehouse_db.startswith(state_dir + os.sep):
        raise RetirementError('warehouse db cannot live inside the sampler state dir')
    return catalog_dir, warehouse_db, sampler_db, output_root


def _load_binding(catalog_dir):
    binding_path = catalog_dir / 'binding.json'
    if not binding_path.exists():
        return None
    try:
        binding = json.loads(binding_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as error:
        raise RetirementError('catalog binding is unreadable: ' + str(error))
    for key in ('version', 'warehouse_db', 'sampler_db', 'output_root',
                'watermarks'):
        if key not in binding:
            raise RetirementError('catalog binding is missing field ' + key)
    if binding['version'] != SCHEMA_VERSION:
        raise RetirementError('unsupported catalog schema version')
    return binding


def ensure_catalog(catalog_dir, warehouse_db, sampler_db, output_root,
                   request_source):
    """Open (or idempotently finish) the catalog binding for one resource set.

    A first, interrupted initialization is recoverable with the same paths
    but cannot activate with only part of the set bound: a request is only
    accepted once every resource advertises the same catalog and the
    binding file is durable.
    """
    catalog_dir, warehouse_db, sampler_db, output_root = _validate_paths(
        catalog_dir, warehouse_db, sampler_db, output_root, request_source)
    catalog_dir.mkdir(parents=True, exist_ok=True)
    binding = _load_binding(catalog_dir)
    wanted = {'warehouse_db': warehouse_db, 'sampler_db': sampler_db,
              'output_root': output_root}
    if binding is not None:
        for key, value in wanted.items():
            if binding[key] != value:
                raise RetirementError(
                    'catalog already binds a different resource for ' + key)
    # Every resource must already be a legal, populated artifact.
    wh_claim = _verify_warehouse_resource(warehouse_db)
    sm_claim = _verify_sampler_resource(sampler_db)
    sampler_conn = sqlite3.connect(sampler_db, timeout=30)
    try:
        last_wm = _read_meta(sampler_conn, 'last_watermark')
        last_wm = None if last_wm is None else int(last_wm)
    finally:
        sampler_conn.close()
    if not Path(output_root).is_dir():
        raise RetirementError('output root does not exist: ' + output_root)
    inflight = _read_inflight(catalog_dir) if binding is not None else {}
    watermarks = _verify_output_tree(Path(output_root), last_wm, inflight)
    # Cross-binding prevention: each resource records the catalog it belongs
    # to; an existing claim for a different catalog is rejected, aliases and
    # repeated init included.
    marker_path = Path(output_root) / OUTPUT_MARKER
    out_claim = None
    if marker_path.exists():
        out_claim = marker_path.read_text(encoding='utf-8').strip()
    claims = [(wh_claim, 'warehouse db'), (sm_claim, 'sampler db'),
              (out_claim, 'output root')]
    catalog_text = str(catalog_dir)
    present = {claim for claim, _ in claims if claim is not None}
    if any(claim != catalog_text for claim in present):
        raise RetirementError('resource is already bound to another catalog')
    if binding is None and present and catalog_text not in present:
        raise RetirementError('partial binding belongs to a different catalog path')
    # Idempotent claim writes (same catalog path), in an order that makes the
    # all-or-nothing binding file the last step.  Explicit transactions are
    # required on default-isolation connections, whose uncommitted changes
    # would otherwise be rolled back at close.
    wh_conn = warehouse_connect(warehouse_db)
    try:
        wh_conn.execute('BEGIN IMMEDIATE')
        _set_meta(wh_conn, CATALOG_META_KEY, catalog_text)
        wh_conn.execute('COMMIT')
    except BaseException:
        _rollback_silent(wh_conn)
        raise
    finally:
        wh_conn.close()
    sm_conn = sqlite3.connect(sampler_db, timeout=30)
    try:
        sm_conn.execute('BEGIN IMMEDIATE')
        _set_meta(sm_conn, CATALOG_META_KEY, catalog_text)
        sm_conn.execute('COMMIT')
    except BaseException:
        _rollback_silent(sm_conn)
        raise
    finally:
        sm_conn.close()
    _write_text_atomic(marker_path, catalog_text + '\n')
    if binding is None:
        binding = {
            'version': SCHEMA_VERSION,
            'warehouse_db': warehouse_db,
            'sampler_db': sampler_db,
            'output_root': output_root,
            'watermarks': [{'watermark': w, 'manifest_sha256': sha}
                           for w, sha in watermarks],
        }
        save_json(catalog_dir / 'binding.json', binding)
    control = catalog_dir / CONTROL_DB
    conn = _connect_control(control)
    try:
        conn.executescript(CONTROL_SCHEMA)
    finally:
        conn.close()
    return binding


# --------------------------------------------------------------------------- #
# entry gates used by warehouse.py and sampler.py
# --------------------------------------------------------------------------- #
class Gate:
    """Read-only view of the control plane for one managed resource."""

    def __init__(self, catalog_dir, kind, resource_path):
        self.catalog_dir = Path(_canonical(catalog_dir))
        self.kind = kind
        self.resource_path = _canonical(resource_path)
        binding = _load_binding(self.catalog_dir)
        if binding is None:
            raise RetirementError(
                'catalog binding is missing; maintenance never finished init')
        if binding.get(kind) != self.resource_path:
            raise RetirementError('resource is not bound to this catalog')
        # The other two bindings must still resolve to existing resources and
        # advertise the same catalog: a half-bound or tampered catalog closes
        # every entry.
        self._check_peer('warehouse_db', binding, CATALOG_META_KEY)
        self._check_peer('sampler_db', binding, CATALOG_META_KEY)
        marker = Path(binding['output_root']) / OUTPUT_MARKER
        if not marker.is_file() \
                or marker.read_text(encoding='utf-8').strip() != str(self.catalog_dir):
            raise RetirementError('output root catalog marker missing or mismatched')
        control = self.catalog_dir / CONTROL_DB
        if not control.is_file():
            raise RetirementError('retirement control database is missing')
        self.conn = _connect_control(control)
        try:
            self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='requests'"
            ).fetchone()
        except sqlite3.Error as error:
            self.conn.close()
            raise RetirementError(
                'retirement control database is invalid: ' + str(error))

    def _check_peer(self, binding_key, binding, meta_key):
        peer = binding[binding_key]
        if not Path(peer).is_file():
            raise RetirementError('bound resource is missing: ' + peer)
        conn = sqlite3.connect(peer, timeout=30)
        try:
            if conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
            ).fetchone() is None:
                raise RetirementError('bound resource is invalid: ' + peer)
            claim = _read_meta(conn, meta_key)
        finally:
            conn.close()
        if claim != str(self.catalog_dir):
            raise RetirementError('bound resource catalog claim mismatched: ' + peer)

    def check_open(self):
        """Raise while any request is active; managed entries stay closed."""
        row = self.conn.execute(
            "SELECT 1 FROM requests WHERE status='active' LIMIT 1").fetchone()
        if row is not None:
            raise RetirementError(
                'retirement maintenance is active; managed entry is closed')

    def is_blocked(self, trace_id):
        return self.conn.execute(
            'SELECT 1 FROM blocked WHERE trace_id=?', (trace_id,)).fetchone() \
            is not None

    def close(self):
        self.conn.close()


def gate_for_db(db_path, kind):
    """Return an open :class:`Gate` for a managed db, or ``None`` if unmanaged."""
    path = Path(db_path)
    if not path.is_file():
        return None
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        missing = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
        ).fetchone() is None
        if missing:
            return None
        catalog = _read_meta(conn, CATALOG_META_KEY)
    finally:
        conn.close()
    if catalog is None:
        return None
    gate = Gate(catalog, kind, path)
    gate.check_open()
    return gate


# --------------------------------------------------------------------------- #
# request acceptance (durable, before any payload work)
# --------------------------------------------------------------------------- #
def _accept_request(conn, request_id, ordered_ids):
    """Persist the request and extend the blocked set; return (row, resumed)."""
    digest = request_set_digest(ordered_ids)
    conn.execute('BEGIN IMMEDIATE')
    try:
        row = conn.execute(
            'SELECT seq,set_sha256,trace_count,status,report_json '
            'FROM requests WHERE request_id=?', (request_id,)).fetchone()
        if row is not None:
            seq, stored_digest, count, status, report_json = row
            if stored_digest != digest or count != len(ordered_ids):
                raise RetirementError(
                    'request_id was already accepted with a different trace set')
            conn.execute('COMMIT')
            return {'seq': seq, 'status': status,
                    'report_json': report_json}, True
        active = conn.execute(
            "SELECT 1 FROM requests WHERE status='active' LIMIT 1").fetchone()
        if active is not None:
            raise RetirementError(
                'another retirement request is still in progress')
        next_seq = conn.execute(
            'SELECT COALESCE(MAX(seq),0)+1 FROM requests').fetchone()[0]
        conn.execute(
            'INSERT INTO requests(request_id,seq,set_sha256,trace_count,status) '
            "VALUES(?,?,?,?,'active')",
            (request_id, next_seq, digest, len(ordered_ids)))
        conn.executemany('INSERT OR IGNORE INTO blocked(trace_id) VALUES(?)',
                         [(tid,) for tid in ordered_ids])
        conn.execute('COMMIT')
    except BaseException:
        _rollback_silent(conn)
        raise
    _crash('accept')
    return {'seq': next_seq, 'status': 'active', 'report_json': None}, False


def _progress_get(conn, request_id, step):
    row = conn.execute(
        'SELECT status,detail_json FROM progress WHERE request_id=? AND step=?',
        (request_id, step)).fetchone()
    if row is None:
        return None
    return row[0], json.loads(row[1])


def _progress_put(conn, request_id, step, status, detail):
    conn.execute(
        'INSERT INTO progress(request_id,step,status,detail_json) VALUES(?,?,?,?) '
        'ON CONFLICT(request_id,step) DO UPDATE SET status=excluded.status,'
        'detail_json=excluded.detail_json',
        (request_id, step, status, json.dumps(detail, sort_keys=True,
                                               ensure_ascii=False)))


def _chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# --------------------------------------------------------------------------- #
# step 1: warehouse cleanup
# --------------------------------------------------------------------------- #
def _clean_warehouse(control, request_id, ordered_ids, warehouse_db):
    """Delete hit traces, revoking sealed contributions and zero BIT nodes."""
    existing = _progress_get(control, request_id, 'warehouse')
    if existing is not None and existing[0] == 'done':
        return existing[1]
    if existing is None:
        # First-run counts are persisted *before* the destructive transactions
        # so a kill mid-cleanup can never make the retry recount less.
        conn = warehouse_connect(warehouse_db)
        try:
            hit = 0
            for chunk in _chunks(ordered_ids, DELETE_CHUNK):
                marks = ','.join('?' * len(chunk))
                hit += conn.execute(
                    'SELECT COUNT(*) FROM traces WHERE trace_id IN (%s)' % marks,
                    chunk).fetchone()[0]
            detail = {'warehouse_traces': hit}
            control.execute('BEGIN IMMEDIATE')
            _progress_put(control, request_id, 'warehouse', 'prepared', detail)
            control.execute('COMMIT')
        finally:
            conn.close()
    else:
        detail = existing[1]
    conn = warehouse_connect(warehouse_db)
    try:
        for chunk in _chunks(ordered_ids, DELETE_CHUNK):
            marks = ','.join('?' * len(chunk))
            conn.execute('BEGIN IMMEDIATE')
            try:
                touched = {}
                targets = conn.execute(
                    'SELECT trace_id,status FROM traces WHERE trace_id IN (%s)'
                    % marks, chunk).fetchall()
                for trace_id, status in targets:
                    if status == 'sealed':
                        # Undo the original Fenwick contributions exactly, then
                        # remove the now-zero sparse nodes.
                        _revoke_trace(conn, trace_id, touched)
                conn.execute(
                    'DELETE FROM spans WHERE trace_id IN (%s)' % marks, chunk)
                conn.execute(
                    'DELETE FROM traces WHERE trace_id IN (%s)' % marks, chunk)
                _cleanup_zero_nodes(conn, touched)
                conn.execute('COMMIT')
            except BaseException:
                _rollback_silent(conn)
                raise
        _crash('warehouse_commit')
    finally:
        conn.close()
    control.execute('BEGIN IMMEDIATE')
    _progress_put(control, request_id, 'warehouse', 'done', detail)
    control.execute('COMMIT')
    return detail


# --------------------------------------------------------------------------- #
# step 2: sampler state cleanup
# --------------------------------------------------------------------------- #
def _clean_sampler(control, request_id, ordered_ids, sampler_db):
    """Delete waiting/finalized traces, spans, finish rows and span evidence."""
    existing = _progress_get(control, request_id, 'sampler')
    if existing is not None and existing[0] == 'done':
        return existing[1]
    if existing is None:
        conn = sqlite3.connect(str(sampler_db), timeout=30, isolation_level=None)
        try:
            waiting = 0
            for chunk in _chunks(ordered_ids, DELETE_CHUNK):
                marks = ','.join('?' * len(chunk))
                waiting += conn.execute(
                    "SELECT COUNT(*) FROM traces WHERE status='waiting' "
                    'AND trace_id IN (%s)' % marks, chunk).fetchone()[0]
            detail = {'sampler_waiting': waiting}
            control.execute('BEGIN IMMEDIATE')
            _progress_put(control, request_id, 'sampler', 'prepared', detail)
            control.execute('COMMIT')
        finally:
            conn.close()
    else:
        detail = existing[1]
    conn = sqlite3.connect(str(sampler_db), timeout=30, isolation_level=None)
    try:
        for chunk in _chunks(ordered_ids, DELETE_CHUNK):
            marks = ','.join('?' * len(chunk))
            conn.execute('BEGIN IMMEDIATE')
            try:
                # Identity metas, offsets and watermarks are never touched.
                conn.execute('DELETE FROM evidence WHERE trace_id IN (%s)'
                             % marks, chunk)
                conn.execute('DELETE FROM spans WHERE trace_id IN (%s)'
                             % marks, chunk)
                conn.execute('DELETE FROM traces WHERE trace_id IN (%s)'
                             % marks, chunk)
                conn.execute('COMMIT')
            except BaseException:
                _rollback_silent(conn)
                raise
        _crash('sampler_commit')
    finally:
        conn.close()
    control.execute('BEGIN IMMEDIATE')
    _progress_put(control, request_id, 'sampler', 'done', detail)
    control.execute('COMMIT')
    return detail


# --------------------------------------------------------------------------- #
# step 3: published watermark cleanup (streaming, per-file replacement)
# --------------------------------------------------------------------------- #
def _filter_jsonl(source, dest, blocked):
    """Stream one JSONL file to ``dest``, dropping records of blocked ids.

    Survivor lines are copied byte-for-byte and keep their original order;
    only whole lines matching a blocked id disappear.  Returns
    ``(removed, count, sha256)`` for the new file.
    """
    removed = 0
    count = 0
    digest = hashlib.sha256()
    with source.open('rb') as src, dest.open('wb') as out:
        for line in src:
            if not line.endswith(b'\n'):
                raise RetirementError('JSONL line missing newline: ' + str(source))
            obj = json.loads(line.decode('utf-8'))
            if obj['trace_id'] in blocked:
                removed += 1
                continue
            out.write(line)
            digest.update(line)
            count += 1
    return removed, count, digest.hexdigest()


def _rebuild_staging(wm_dir, staging_wm, detail, blocked):
    """Regenerate staged final files from the current visible tree.

    A visible file is either already the replacement (its digest equals the
    durable target) or still the original (its digest matches the stale
    manifest and contains the blocked records).  The first kind is carried
    through byte-for-byte; the second is filtered.  Either way the rebuilt
    bytes must equal the durable prepared target, whose first-run counts stay
    authoritative and are never recomputed.
    """
    staging_wm.mkdir(parents=True, exist_ok=True)
    for name in OUTPUT_FILES:
        staged = staging_wm / (name + '.tmp')
        visible = wm_dir / name
        wanted = detail['files'][name]
        if _sha256_file(visible) == wanted['sha256']:
            with visible.open('rb') as src, staged.open('wb') as out:
                for chunk in iter(lambda: src.read(1 << 20), b''):
                    out.write(chunk)
            if _count_lines(staged) != wanted['count']:
                raise RetirementError(
                    'in-flight replacement count diverged for ' + name)
        else:
            removed, count, sha = _filter_jsonl(visible, staged, blocked)
            if sha != wanted['sha256'] or count != wanted['count']:
                raise RetirementError(
                    'in-flight cleanup cannot reconstruct target for ' + name)


def _count_lines(path):
    total = 0
    with Path(path).open('rb') as stream:
        for _ in stream:
            total += 1
    return total


def _watermark_step(control, request_id, value, wm_dir, staging_wm, blocked,
                    binding, index):
    """Clean one watermark tree; idempotent and resumable across replacements."""
    step = 'output-%d' % value
    existing = _progress_get(control, request_id, step)
    if existing is not None and existing[0] == 'done':
        return existing[1]
    if existing is not None and existing[0] == 'prepared':
        # First-run counts are durable and authoritative.  Rebuild the staged
        # files from whatever currently sits in the visible tree, so a kill at
        # any point between staging and the final manifest replacement can be
        # resumed without recounting partially deleted input.
        detail = existing[1]
        _rebuild_staging(wm_dir, staging_wm, detail, blocked)
    else:
        staging_wm.mkdir(parents=True, exist_ok=True)
        detail = {'watermark': value, 'files': {}}
        for name in OUTPUT_FILES:
            removed, count, sha = _filter_jsonl(
                wm_dir / name, staging_wm / (name + '.tmp'), blocked)
            detail['files'][name] = {'removed': removed, 'count': count,
                                     'sha256': sha}
        control.execute('BEGIN IMMEDIATE')
        _progress_put(control, request_id, step, 'prepared', detail)
        control.execute('COMMIT')
    _crash('wmbefore', value)
    totals = {name: detail['files'][name]['removed'] for name in OUTPUT_FILES}
    if any(totals.values()):
        # Replace data files first; the manifest is republished only after all
        # three replacements, so a kill here leaves a repairable tree whose
        # stale manifest is detected and fixed on retry.
        for name in OUTPUT_FILES:
            if detail['files'][name]['removed']:
                os.replace(staging_wm / (name + '.tmp'), wm_dir / name)
        _crash('wmafterfiles', value)
        manifest_path = wm_dir / 'manifest.json'
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        for name in OUTPUT_FILES:
            info = detail['files'][name]
            manifest['files'][name] = {'sha256': info['sha256'],
                                       'count': info['count']}
        save_json(staging_wm / 'manifest.tmp', manifest)
        os.replace(staging_wm / 'manifest.tmp', manifest_path)
    else:
        # No hit: every byte of this directory stays untouched.
        for name in OUTPUT_FILES:
            tmp = staging_wm / (name + '.tmp')
            if tmp.exists():
                tmp.unlink()
    _crash('wmdone', value)
    control.execute('BEGIN IMMEDIATE')
    _progress_put(control, request_id, step, 'done', detail)
    control.execute('COMMIT')
    return detail


def _clean_outputs(control, request_id, ordered_ids, binding, catalog_dir):
    summary = _progress_get(control, request_id, 'output')
    if summary is not None and summary[0] == 'done':
        return summary[1]
    blocked = set(ordered_ids)
    output_root = Path(binding['output_root'])
    staging_root = catalog_dir / ('staging-%s' % request_id)
    details = []
    entries = sorted(output_root.iterdir(), key=lambda p: p.name)
    for entry in entries:
        if entry.name == OUTPUT_MARKER:
            continue
        match = WATERMARK_RE.fullmatch(entry.name)
        if match is None:
            raise RetirementError(
                'unexpected entry in managed output root: ' + entry.name)
        value = int(match.group(1))
        details.append(_watermark_step(
            control, request_id, value, entry,
            staging_root / ('watermark-%d' % value), blocked, binding,
            len(details)))
    totals = {name: sum(d['files'][name]['removed'] for d in details)
              for name in OUTPUT_FILES}
    detail = {'watermarks': len(details), 'removed': totals}
    control.execute('BEGIN IMMEDIATE')
    _progress_put(control, request_id, 'output', 'done', detail)
    control.execute('COMMIT')
    return detail


# --------------------------------------------------------------------------- #
# main entry
# --------------------------------------------------------------------------- #
def retire_traces(catalog_dir, db_path, state_dir, output_dir, source_path):
    """Run one targeted retirement request to completion (idempotent retry).

    ``db_path`` is the managed warehouse database, ``state_dir`` the sampler
    state directory (holding ``sampler.db``), ``output_dir`` the sampler
    output root and ``source_path`` the read-only strict JSON request file.
    Returns the completion report; raises :class:`RetirementError` (CLI exit
    2) on any contract violation.
    """
    data, source_sha = read_json(source_path)
    request_id, ordered_ids = validate_request(data)
    sampler_db = Path(state_dir) / 'sampler.db'
    binding = ensure_catalog(catalog_dir, db_path, sampler_db, output_dir,
                             source_path)
    control = _connect_control(Path(catalog_dir) / CONTROL_DB)
    staging_root = Path(catalog_dir) / ('staging-%s' % request_id)
    try:
        row, _resumed = _accept_request(control, request_id, ordered_ids)
        if row['status'] == 'done':
            # Sweep any staging left by a process killed right after the
            # completion marker but before its own cleanup ran.
            if staging_root.exists():
                _rmtree(staging_root)
            return json.loads(row['report_json'])
        seq = row['seq']
        wh_detail = _clean_warehouse(
            control, request_id, ordered_ids, binding['warehouse_db'])
        sm_detail = _clean_sampler(
            control, request_id, ordered_ids, binding['sampler_db'])
        out_detail = _clean_outputs(
            control, request_id, ordered_ids, binding, Path(catalog_dir))
        report = {
            'request_id': request_id,
            'seq': seq,
            'status': 'done',
            'trace_count': len(ordered_ids),
            'warehouse_traces': wh_detail['warehouse_traces'],
            'sampler_waiting': sm_detail['sampler_waiting'],
            'published_kept': out_detail['removed']['kept.jsonl'],
            'published_dropped': out_detail['removed']['dropped.jsonl'],
            'published_quarantined': out_detail['removed']['quarantine.jsonl'],
        }
        _crash('complete')
        control.execute('BEGIN IMMEDIATE')
        try:
            control.execute(
                "UPDATE requests SET status='done',report_json=? WHERE request_id=?",
                (json.dumps(report, sort_keys=True, ensure_ascii=False),
                 request_id))
            control.execute('COMMIT')
        except BaseException:
            _rollback_silent(control)
            raise
    finally:
        control.close()
    # Staging space only exists for an in-flight request; remove it once the
    # completion marker is durable.  On a later retry the request is already
    # done and returns above, so sweep any leftover here as well.
    if staging_root.exists():
        _rmtree(staging_root)
    return report


def _rmtree(path):
    import shutil
    shutil.rmtree(path, ignore_errors=True)
