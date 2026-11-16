"""Persistent, revision-aware SLO alerting on top of the offline warehouse.

The module adds a per-root-service request fact index and a durable alert /
notification-lease work queue to an existing warehouse SQLite file.  Every
write happens inside the caller's warehouse transaction (``BEGIN
IMMEDEDIATE``) so request facts, occupancy data, ``max_seq``, batch identity,
alert states and notifications commit or roll back together.

Request window index
--------------------

Each sealed trace contributes exactly one request fact, supplied by its unique
root span: ``(service = root.service, end = root.end, slow =
root.end-root.start >= slow_ticks)``.  Facts are ordered by
``(end ASC, trace_id UTF-8 bytes ASC)`` and kept in a deterministic AVL
order-statistics tree per service (table ``slo_requests``).  The tree is a
*revisable* index, never an append-only queue: an old fact is deleted and the
new one inserted on every revision (root time/service change, reopen,
reseal), at O(log M) cost, where M is the service's current fact count.
Every node caches its subtree size and slow count, so the slow counts of the
last ``short_n`` / ``long_n`` facts (the two SLO windows) are suffix
order-statistics queries costing O(log M) each -- no scan over N, no scan of
other traces' spans and no full service rebuild.

Notifications and leases
------------------------

Every healthy->firing (``open``) and firing->healthy (``close``) transition
appends exactly one immutable notification with a library-wide continuous
``event_id`` and the window counts captured at the transition.  A
lease/ack work queue (``slo_leases`` / ``slo_ack_evidence``) gives offline
consumers at-least-once delivery with rotating tokens, expiring leases and
idempotent acks.  Acked notifications keep their rows forever as idempotency
evidence but stop counting against the unacked ``capacity``.

Only the Python standard library is used.
"""
from pathlib import Path
import hashlib
import json
import os
import sqlite3

from .common import require, ident

MAX_TICKS = 10**12
MAX_POLICIES = 100
MAX_CAPACITY = 10_000
MAX_WINDOW_N = 10**6
MAX_ACK_ITEMS = 100
MAX_CLAIM_LIMIT = 100
MAX_LEASE_TICKS = 10**6
PPM = 10**6
SLO_VERSION = '1'
META_PREFIX = 'slo_'


# --------------------------------------------------------------------------- #
# test-only fault injection
# --------------------------------------------------------------------------- #
def crash_hook(stage):
    """Kill the process without cleanup at a named durable boundary.

    ``TRACE_LATENCY_SLO_CRASH`` is test-only.  Stages:
    ``init_commit`` (before the SLO-enabling transaction commits),
    ``batch_commit`` (inside a warehouse batch right before COMMIT),
    ``claim_commit`` and ``ack_commit`` (before each delivery transaction
    commits).
    """
    if os.environ.get('TRACE_LATENCY_SLO_CRASH') == stage:
        os._exit(1)


class SloError(ValueError):
    """An SLO contract violation (CLI exit 2)."""


# --------------------------------------------------------------------------- #
# strict validation
# --------------------------------------------------------------------------- #
def _int(value, name, minimum, maximum):
    # ``type(...) is int`` rejects ``bool`` as required.
    if type(value) is not int or not minimum <= value <= maximum:
        raise SloError(name + ' must be an integer in %d..%d (bool rejected)'
                       % (minimum, maximum))
    return value


def validate_config(data):
    """Strictly validate ``{capacity, policies}``; return the normalised list."""
    require(data, 'capacity policies')
    capacity = _int(data['capacity'], 'capacity', 1, MAX_CAPACITY)
    policies = data['policies']
    if not isinstance(policies, list) or not 1 <= len(policies) <= MAX_POLICIES:
        raise SloError('policies must be an array of 1..%d objects' % MAX_POLICIES)
    seen = set()
    normalised = []
    for index, item in enumerate(policies):
        if not isinstance(item, dict):
            raise SloError('policy %d must be an object' % index)
        require(item, 'service slow_ticks short_n long_n open_ppm close_ppm')
        service = ident(item['service'])
        if service in seen:
            raise SloError('duplicate policy service: ' + service)
        seen.add(service)
        slow_ticks = _int(item['slow_ticks'], 'slow_ticks', 0, MAX_TICKS)
        short_n = _int(item['short_n'], 'short_n', 1, MAX_WINDOW_N)
        long_n = _int(item['long_n'], 'long_n', 1, MAX_WINDOW_N)
        if short_n > long_n:
            raise SloError('short_n must be <= long_n')
        open_ppm = _int(item['open_ppm'], 'open_ppm', 0, PPM)
        close_ppm = _int(item['close_ppm'], 'close_ppm', 0, PPM)
        if not close_ppm < open_ppm:
            raise SloError('close_ppm must be < open_ppm')
        normalised.append({'service': service, 'slow_ticks': slow_ticks,
                           'short_n': short_n, 'long_n': long_n,
                           'open_ppm': open_ppm, 'close_ppm': close_ppm})
    return capacity, normalised


def validate_claim(data):
    require(data, 'consumer now lease_ticks limit')
    consumer = ident(data['consumer'])
    now = _int(data['now'], 'now', 0, MAX_TICKS)
    lease_ticks = _int(data['lease_ticks'], 'lease_ticks', 1, MAX_LEASE_TICKS)
    if now + lease_ticks > MAX_TICKS:
        raise SloError('now + lease_ticks must be <= 10^12')
    limit = _int(data['limit'], 'limit', 1, MAX_CLAIM_LIMIT)
    return consumer, now, lease_ticks, limit


def validate_ack(data):
    require(data, 'consumer now items')
    consumer = ident(data['consumer'])
    now = _int(data['now'], 'now', 0, MAX_TICKS)
    items = data['items']
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_ACK_ITEMS:
        raise SloError('items must be an array of 1..%d objects' % MAX_ACK_ITEMS)
    seen = set()
    normalised = []
    for item in items:
        require(item, 'event_id token')
        event_id = _int(item['event_id'], 'event_id', 1, 10**18)
        token = ident(item['token'])
        if event_id in seen:
            raise SloError('ack items must have distinct event_id values')
        seen.add(event_id)
        normalised.append((event_id, token))
    return consumer, now, normalised


def config_digest(data):
    raw = json.dumps(data, sort_keys=True, separators=(',', ':'),
                     ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


# --------------------------------------------------------------------------- #
# schema
# --------------------------------------------------------------------------- #
SCHEMA = """
CREATE TABLE IF NOT EXISTS slo_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS slo_policies (
    service TEXT PRIMARY KEY,
    slow_ticks INTEGER NOT NULL,
    short_n INTEGER NOT NULL,
    long_n INTEGER NOT NULL,
    open_ppm INTEGER NOT NULL,
    close_ppm INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS slo_requests (
    service TEXT NOT NULL,
    node_id INTEGER NOT NULL,
    parent_id INTEGER,
    left_id INTEGER,
    right_id INTEGER,
    height INTEGER NOT NULL,
    subtree_size INTEGER NOT NULL,
    slow_count INTEGER NOT NULL,
    end_tick INTEGER NOT NULL,
    trace_bytes BLOB NOT NULL,
    trace_id TEXT NOT NULL,
    slow INTEGER NOT NULL CHECK (slow IN (0,1)),
    PRIMARY KEY (service, node_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_slo_requests_trace
    ON slo_requests(service, trace_id);
-- No UNIQUE index on (service, end_tick, trace_bytes): a two-child AVL
-- deletion copies the successor's ordered key onto the victim node while the
-- successor row still exists, so equal keys exist transiently inside one
-- transaction.  Key uniqueness is an invariant enforced by OrderTree.insert
-- (duplicate keys are rejected) and the per-trace one-fact rule.
CREATE INDEX IF NOT EXISTS idx_slo_requests_order
    ON slo_requests(service, end_tick, trace_bytes);
CREATE TABLE IF NOT EXISTS slo_service_state (
    service TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('healthy','firing'))
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS slo_notifications (
    event_id INTEGER PRIMARY KEY,
    service TEXT NOT NULL,
    transition TEXT NOT NULL CHECK (transition IN ('open','close')),
    max_seq INTEGER NOT NULL,
    short_bad INTEGER NOT NULL,
    short_total INTEGER NOT NULL,
    long_bad INTEGER NOT NULL,
    long_total INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS slo_leases (
    event_id INTEGER PRIMARY KEY,
    consumer TEXT NOT NULL,
    token TEXT NOT NULL,
    expires_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS slo_ack_evidence (
    event_id INTEGER PRIMARY KEY,
    consumer TEXT NOT NULL,
    token TEXT NOT NULL,
    acked_at INTEGER NOT NULL
) WITHOUT ROWID;
"""


def ensure_slo_schema(conn):
    # Individual ``execute`` calls (not ``executescript``): executescript
    # issues an implicit COMMIT and would break the single-transaction
    # atomicity guarantees of slo_init / the warehouse batch hook.
    for statement in SCHEMA.split(';'):
        statement = statement.strip()
        if statement:
            conn.execute(statement)


def is_enabled(conn):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='slo_meta'"
    ).fetchone()
    if row is None:
        return False
    return conn.execute(
        "SELECT 1 FROM slo_meta WHERE key='enabled'").fetchone() is not None


def require_enabled(conn):
    if not is_enabled(conn):
        raise SloError('SLO pipeline is not enabled on this warehouse')


def _meta_get(conn, key):
    row = conn.execute('SELECT value FROM slo_meta WHERE key=?',
                       (META_PREFIX + key,)).fetchone()
    return None if row is None else row[0]


def _meta_set(conn, key, value):
    conn.execute(
        'INSERT INTO slo_meta(key,value) VALUES(?,?) '
        'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
        (META_PREFIX + key, str(value)))


# --------------------------------------------------------------------------- #
# per-service AVL order-statistics tree
# --------------------------------------------------------------------------- #
def _key_less(end_a, bytes_a, end_b, bytes_b):
    if end_a != end_b:
        return end_a < end_b
    return bytes_a < bytes_b


class OrderTree:
    """AVL order-statistics tree for one service's request facts.

    Node identity is ``(service, node_id)``; node ids are allocated densely
    per service and never reused (deletions free ids via a counter-free
    ``COALESCE(MAX(node_id)+1,1)`` allocation, and AVL rotations never delete
    rows, so ids stay unique).  All operations walk at most O(log M) nodes.
    """

    def __init__(self, conn, service):
        self.conn = conn
        self.service = service

    # -- low level --------------------------------------------------------- #
    def _row(self, node_id):
        if node_id is None:
            return None
        return self.conn.execute(
            'SELECT node_id,parent_id,left_id,right_id,height,'
            'subtree_size,slow_count,end_tick,trace_bytes,trace_id,slow '
            'FROM slo_requests WHERE service=? AND node_id=?',
            (self.service, node_id)).fetchone()

    def _root_id(self):
        row = self.conn.execute(
            'SELECT node_id FROM slo_requests WHERE service=? AND parent_id IS NULL',
            (self.service,)).fetchone()
        return None if row is None else row[0]

    @staticmethod
    def _h(row):
        return 0 if row is None else row[4]

    @staticmethod
    def _sz(row):
        return 0 if row is None else row[5]

    @staticmethod
    def _slow(row):
        return 0 if row is None else row[6]

    def _write_node(self, node_id, parent_id, left_id, right_id, height,
                    size, slow_sum):
        self.conn.execute(
            'UPDATE slo_requests SET parent_id=?,left_id=?,right_id=?,'
            'height=?,subtree_size=?,slow_count=? WHERE service=? AND node_id=?',
            (parent_id, left_id, right_id, height, size, slow_sum,
             self.service, node_id))

    def _refresh_node(self, node_id):
        """Recompute aggregates from the current children; return the row."""
        row = self._row(node_id)
        left = self._row(row[2])
        right = self._row(row[3])
        height = 1 + max(self._h(left), self._h(right))
        size = 1 + self._sz(left) + self._sz(right)
        slow_sum = row[10] + self._slow(left) + self._slow(right)
        self._write_node(node_id, row[1], row[2], row[3], height, size,
                         slow_sum)
        return self._row(node_id)

    def _set_left(self, node_id, child_id):
        self.conn.execute(
            'UPDATE slo_requests SET left_id=? WHERE service=? AND node_id=?',
            (child_id, self.service, node_id))

    def _set_right(self, node_id, child_id):
        self.conn.execute(
            'UPDATE slo_requests SET right_id=? WHERE service=? AND node_id=?',
            (child_id, self.service, node_id))

    def _set_parent(self, node_id, parent_id):
        self.conn.execute(
            'UPDATE slo_requests SET parent_id=? WHERE service=? AND node_id=?',
            (parent_id, self.service, node_id))

    def _rotate_left(self, node_id, parent_id):
        """Left-rotate; return the new subtree root (parent set by caller)."""
        row = self._row(node_id)
        right_id = row[3]
        right = self._row(right_id)
        rl_id = right[2]
        self._set_right(node_id, rl_id)
        if rl_id is not None:
            self._set_parent(rl_id, node_id)
        self._set_left(right_id, node_id)
        self._set_parent(node_id, right_id)
        self._set_parent(right_id, parent_id)
        self._refresh_node(node_id)
        self._refresh_node(right_id)
        return right_id

    def _rotate_right(self, node_id, parent_id):
        row = self._row(node_id)
        left_id = row[2]
        left = self._row(left_id)
        lr_id = left[3]
        self._set_left(node_id, lr_id)
        if lr_id is not None:
            self._set_parent(lr_id, node_id)
        self._set_right(left_id, node_id)
        self._set_parent(node_id, left_id)
        self._set_parent(left_id, parent_id)
        self._refresh_node(node_id)
        self._refresh_node(left_id)
        return left_id

    def _rebalance(self, node_id, parent_id):
        """Restore AVL balance; return the (possibly new) subtree root."""
        row = self._refresh_node(node_id)
        left = self._row(row[2])
        right = self._row(row[3])
        balance = self._h(left) - self._h(right)
        if balance > 1:
            if self._h(self._row(left[2])) < self._h(self._row(left[3])):
                # Left-right: inner left rotation on the left child.
                new_left = self._rotate_left(left[0], node_id)
                self._set_left(node_id, new_left)
            return self._rotate_right(node_id, parent_id)
        if balance < -1:
            if self._h(self._row(right[2])) > self._h(self._row(right[3])):
                # Right-left: inner right rotation on the right child.
                new_right = self._rotate_right(right[0], node_id)
                self._set_right(node_id, new_right)
            return self._rotate_left(node_id, parent_id)
        return node_id

    # -- mutations --------------------------------------------------------- #
    def _alloc_node_id(self):
        # A monotonic per-service high-water mark: node ids are never reused,
        # so deleting a node and later inserting a fresh fact (or the same
        # trace again) can never collide with a freed primary key.  The mark
        # itself is stored in ``slo_meta`` and survives deletions.
        key = META_PREFIX + 'node_seq:' + self.service
        row = self.conn.execute('SELECT value FROM slo_meta WHERE key=?',
                                (key,)).fetchone()
        node_id = 1 if row is None else int(row[0]) + 1
        self.conn.execute(
            'INSERT INTO slo_meta(key,value) VALUES(?,?) '
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (key, str(node_id)))
        return node_id

    def insert(self, end_tick, trace_id, slow):
        trace_bytes = trace_id.encode('utf-8')
        slow_int = 1 if slow else 0
        root_id = self._root_id()
        if root_id is None:
            node_id = self._alloc_node_id()
            self.conn.execute(
                'INSERT INTO slo_requests(service,node_id,parent_id,left_id,'
                'right_id,height,subtree_size,slow_count,end_tick,trace_bytes,'
                'trace_id,slow) VALUES(?,?,NULL,NULL,NULL,1,1,?,?,?,?,?)',
                (self.service, node_id, slow_int, end_tick, trace_bytes, trace_id,
                 slow_int))
            return
        # Recursive insertion: each level returns the root of its subtree after
        # rebalancing, and the caller adopts it into its child slot.  The
        # recursion depth is bounded by the AVL height (<= ~24 for 10^6).
        def rec(node_id, parent_id):
            row = self._row(node_id)
            if _key_less(end_tick, trace_bytes, row[7], row[8]):
                if row[2] is None:
                    new_id = self._alloc_node_id()
                    self.conn.execute(
                        'INSERT INTO slo_requests(service,node_id,parent_id,'
                        'left_id,right_id,height,subtree_size,slow_count,'
                        'end_tick,trace_bytes,trace_id,slow) '
                        'VALUES(?,?,?,NULL,NULL,1,1,?,?,?,?,?)',
                        (self.service, new_id, node_id, slow_int, end_tick,
                         trace_bytes, trace_id, slow_int))
                    self._set_left(node_id, new_id)
                else:
                    new_left = rec(row[2], node_id)
                    self._set_left(node_id, new_left)
                    self._set_parent(new_left, node_id)
            elif _key_less(row[7], row[8], end_tick, trace_bytes):
                if row[3] is None:
                    new_id = self._alloc_node_id()
                    self.conn.execute(
                        'INSERT INTO slo_requests(service,node_id,parent_id,'
                        'left_id,right_id,height,subtree_size,slow_count,'
                        'end_tick,trace_bytes,trace_id,slow) '
                        'VALUES(?,?,?,NULL,NULL,1,1,?,?,?,?,?)',
                        (self.service, new_id, node_id, slow_int, end_tick,
                         trace_bytes, trace_id, slow_int))
                    self._set_right(node_id, new_id)
                else:
                    new_right = rec(row[3], node_id)
                    self._set_right(node_id, new_right)
                    self._set_parent(new_right, node_id)
            else:
                raise SloError(
                    'duplicate SLO request fact for trace ' + trace_id)
            return self._rebalance(node_id, parent_id)

        new_root = rec(root_id, None)
        self._set_parent(new_root, None)

    def find_by_trace(self, trace_id):
        row = self.conn.execute(
            'SELECT node_id,end_tick,trace_bytes,slow FROM slo_requests '
            'WHERE service=? AND trace_id=?',
            (self.service, trace_id)).fetchone()
        return row

    def delete_fact(self, trace_id):
        """Delete one fact, keeping every surviving trace's node identity.

        Standard recursive AVL deletion by node id:

        * a leaf / one-child node is spliced out directly;
        * a two-child victim adopts its in-order successor's ordered key/fact
          but keeps its own node id and child links, and the successor *node*
          (which has at most a right child) is the node physically removed.

        The recursion therefore always removes a node with <= 1 child and
        rebalances on the unwind, while the victim node's own ``slow`` flag is
        refreshed before its aggregates are recomputed -- no fact is counted
        twice or lost.  Because the successor's fact moves into the victim's
        node, the surviving successor trace remains findable by trace id.
        """
        victim = self.find_by_trace(trace_id)
        if victim is None:
            return
        victim_id = victim[0]
        victim_row = self._row(victim_id)
        if victim_row[2] is not None and victim_row[3] is not None:
            # In-order successor: leftmost node of the right subtree; it
            # always has at most a right child.
            succ = self._row(victim_row[3])
            while succ[2] is not None:
                succ = self._row(succ[2])
            succ_fact = (succ[7], succ[8], succ[9], succ[10])
            # 1) Physically remove the successor node and rebalance the right
            #    subtree first, while every node still holds its original
            #    ordered key (no duplicate key exists during the descent).
            new_right = self._remove_descendant(victim_row[3], victim_id,
                                                 succ[0])
            self._set_right(victim_id, new_right)
            if new_right is not None:
                self._set_parent(new_right, victim_id)
            # 2) Only now adopt the successor's ordered key/fact on the victim
            #    node (id and child links unchanged), then rebalance the victim
            #    and every ancestor against the final (shrunk) child structure.
            self.conn.execute(
                'UPDATE slo_requests SET end_tick=?,trace_bytes=?,'
                'trace_id=?,slow=? WHERE service=? AND node_id=?',
                (succ_fact[0], succ_fact[1], succ_fact[2], succ_fact[3],
                 self.service, victim_id))
            new_root = self._rebalance_to_root(victim_id)
        else:
            new_root = self._remove_descendant(self._root_id(), None,
                                               victim_id)
        if new_root is not None:
            self._set_parent(new_root, None)

    def _remove_descendant(self, root_id, root_parent_id, remove_id):
        """Remove node ``remove_id`` (with <= 1 child); return subtree root."""
        def rec(node_id, parent_id):
            row = self._row(node_id)
            if node_id == remove_id:
                child_id = row[2] if row[2] is not None else row[3]
                if child_id is not None:
                    self._set_parent(child_id, parent_id)
                self.conn.execute(
                    'DELETE FROM slo_requests WHERE service=? AND node_id=?',
                    (self.service, node_id))
                return child_id
            target = self._row(remove_id)
            if _key_less(target[7], target[8], row[7], row[8]):
                new_left = rec(row[2], node_id)
                self._set_left(node_id, new_left)
                if new_left is not None:
                    self._set_parent(new_left, node_id)
            else:
                new_right = rec(row[3], node_id)
                self._set_right(node_id, new_right)
                if new_right is not None:
                    self._set_parent(new_right, node_id)
            return self._rebalance(node_id, parent_id)
        return rec(root_id, root_parent_id)

    def _rebalance_to_root(self, node_id):
        """Rebalance (and refresh aggregates) from ``node_id`` up to the root.

        Parent pointers are stable for the nodes on this path because no
        rotation has happened yet at these levels; each rebalance only changes
        the subtree root below the current node, which is re-adopted through
        the parent's live child slot.  Returns the resulting tree root id.
        """
        cur = node_id
        root_id = node_id
        while cur is not None:
            row = self._row(cur)
            parent_id = row[1]
            new_root = self._rebalance(cur, parent_id)
            if parent_id is not None:
                parent = self._row(parent_id)
                if parent[2] == cur:
                    self._set_left(parent_id, new_root)
                else:
                    self._set_right(parent_id, new_root)
            root_id = new_root
            cur = parent_id
        return root_id


    # -- reads ------------------------------------------------------------- #
    def total(self):
        root_id = self._root_id()
        if root_id is None:
            return 0, 0
        root = self._row(root_id)
        return root[5], root[6]

    def prefix_stats(self, k):
        """Aggregate over the first ``k`` in-order nodes (prefix), O(height).

        A single 0-based rank walk toward the node with in-order rank
        ``k-1``.  On every right descent the current node and its whole left
        subtree precede the target and are added via cached aggregates in
        O(1); the boundary node itself is added when its rank is reached.
        Returns ``(count, slow)`` with ``count == min(k, M)``.
        """
        root_id = self._root_id()
        if root_id is None or k <= 0:
            return 0, 0
        root = self._row(root_id)
        total_size = root[5]
        if k >= total_size:
            return root[5], root[6]
        rank = k - 1                      # 0-based rank of the last prefix node
        count = 0
        slow = 0
        node = root
        while node is not None:
            left = self._row(node[2])
            left_size = self._sz(left)
            if rank < left_size:
                node = left
            elif rank == left_size:
                return count + 1 + left_size, \
                    slow + node[10] + self._slow(left)
            else:
                # The node plus its whole left subtree are inside the prefix.
                count += 1 + left_size
                slow += node[10] + self._slow(left)
                rank -= left_size + 1
                node = self._row(node[3])
        return count, slow

    def suffix_stats(self, n):
        """Return ``(count, slow)`` over the last min(n, M) facts in order.

        The suffix of the last ``take`` nodes is the whole tree minus the
        prefix of the first ``total-take`` nodes, so two O(height)
        order-statistics walks give the exact window totals.  Neither the
        window (size N) nor the other M facts are ever scanned.
        """
        root_id = self._root_id()
        if root_id is None or n <= 0:
            return 0, 0
        root = self._row(root_id)
        total_size, total_slow = root[5], root[6]
        take = min(n, total_size)
        if take == total_size:
            return total_size, total_slow
        prefix_count, prefix_slow = self.prefix_stats(total_size - take)
        return total_size - prefix_count, total_slow - prefix_slow

    def ordered_trace_ids(self):
        """In-order scan; used by initialization and tests, never hot paths."""
        out = []

        def walk(node_id):
            if node_id is None:
                return
            row = self._row(node_id)
            walk(row[2])
            out.append((row[7], row[9], bool(row[10])))
            walk(row[3])

        walk(self._root_id())
        return out


# --------------------------------------------------------------------------- #
# root request facts
# --------------------------------------------------------------------------- #
def policy_map(conn):
    return {row[0]: {'service': row[0], 'slow_ticks': row[1],
                     'short_n': row[2], 'long_n': row[3],
                     'open_ppm': row[4], 'close_ppm': row[5]}
            for row in conn.execute(
                'SELECT service,slow_ticks,short_n,long_n,open_ppm,close_ppm '
                'FROM slo_policies')}


def service_state(conn, service):
    row = conn.execute(
        'SELECT state FROM slo_service_state WHERE service=?',
        (service,)).fetchone()
    return 'healthy' if row is None else row[0]


def _set_state(conn, service, state):
    conn.execute(
        'INSERT INTO slo_service_state(service,state) VALUES(?,?) '
        'ON CONFLICT(service) DO UPDATE SET state=excluded.state',
        (service, state))


def _ratio_at_least(bad, total, ppm):
    # bad/total >= ppm/10^6 via exact integer cross multiplication.
    return bad * PPM >= ppm * total


def evaluate_service(conn, service, policy, max_seq):
    """Recompute windows/state for one service; append one flip notification.

    Returns the appended notification row (as a dict) or ``None`` when the
    state is unchanged.
    """
    tree = OrderTree(conn, service)
    long_total, long_bad = tree.suffix_stats(policy['long_n'])
    short_total, short_bad = tree.suffix_stats(policy['short_n'])
    long_full = long_total >= policy['long_n']
    previous = service_state(conn, service)
    if previous == 'healthy':
        firing = (long_full
                  and _ratio_at_least(short_bad, short_total, policy['open_ppm'])
                  and _ratio_at_least(long_bad, long_total, policy['open_ppm']))
        transition = 'open' if firing else None
    else:
        # While firing: any window's rate <= close_ppm, or a no-longer-full
        # long window, flips back to healthy.  The <= comparisons are also
        # exact integer cross multiplications.
        short_close = short_bad * PPM <= policy['close_ppm'] * short_total
        long_close = long_bad * PPM <= policy['close_ppm'] * long_total
        firing = long_full and not short_close and not long_close
        transition = 'close' if not firing else None
    if transition is None:
        return None
    new_state = 'firing' if transition == 'open' else 'healthy'
    event_id = conn.execute(
        'SELECT COALESCE(MAX(event_id),0)+1 FROM slo_notifications').fetchone()[0]
    conn.execute(
        'INSERT INTO slo_notifications(event_id,service,transition,max_seq,'
        'short_bad,short_total,long_bad,long_total) VALUES(?,?,?,?,?,?,?,?)',
        (event_id, service, transition, max_seq, short_bad, short_total,
         long_bad, long_total))
    _set_state(conn, service, new_state)
    return {'event_id': event_id, 'service': service,
            'transition': transition, 'max_seq': max_seq,
            'short_bad': short_bad, 'short_total': short_total,
            'long_bad': long_bad, 'long_total': long_total}


def _unacked_count(conn):
    return conn.execute(
        'SELECT COUNT(*) FROM slo_notifications n WHERE NOT EXISTS ('
        'SELECT 1 FROM slo_ack_evidence a WHERE a.event_id=n.event_id)'
    ).fetchone()[0]


# --------------------------------------------------------------------------- #
# warehouse batch hook (runs inside the warehouse write transaction)
# --------------------------------------------------------------------------- #
def pre_batch_snapshot(conn, trace_ids):
    """Old request facts for every touched trace, keyed by trace_id."""
    old = {}
    for trace_id in trace_ids:
        row = conn.execute(
            'SELECT service,end_tick,trace_id,slow FROM slo_requests '
            'WHERE trace_id=?', (trace_id,)).fetchone()
        if row is not None:
            old[trace_id] = {'service': row[0], 'end': row[1],
                             'slow': bool(row[3])}
    return old


def apply_batch_hook(conn, touched_trace_ids, new_max_seq, old_facts):
    """Revise facts/windows/states/notifications for one warehouse batch.

    Called from ``warehouse.apply_locked`` on the same open write
    transaction, after the span/trace changes are final but before COMMIT.
    The whole batch rolls back if this raises (including the capacity gate).
    """
    if not is_enabled(conn):
        return {'enabled': False, 'notifications': []}
    policies = policy_map(conn)
    affected_services = set()
    for trace_id in touched_trace_ids:
        sealed = conn.execute(
            "SELECT 1 FROM traces WHERE trace_id=? AND status='sealed'",
            (trace_id,)).fetchone() is not None
        old = old_facts.get(trace_id)
        new = None
        if sealed:
            # The trace has exactly one root (warehouse seal validation).
            root = conn.execute(
                'SELECT start,end,service FROM spans WHERE trace_id=? '
                'AND parent IS NULL', (trace_id,)).fetchone()
            if root is not None and root[2] in policies:
                start, end, service = root
                new = {'service': service, 'end': end,
                       'slow': (end - start)
                               >= policies[service]['slow_ticks']}
        if old is not None:
            affected_services.add(old['service'])
        if new is not None:
            affected_services.add(new['service'])
        if old == new:
            continue
        if old is not None:
            OrderTree(conn, old['service']).delete_fact(trace_id)
        if new is not None:
            OrderTree(conn, new['service']).insert(
                new['end'], trace_id, new['slow'])
    notifications = []
    for service in sorted(affected_services,
                          key=lambda s: s.encode('utf-8')):
        if service not in policies:
            continue
        notice = evaluate_service(conn, service, policies[service], new_max_seq)
        if notice is not None:
            notifications.append(notice)
    # Reverse capacity gate: the batch may commit only when the current
    # unacked count plus the final new notifications of this batch fit.
    if notifications:
        unacked = _unacked_count(conn)
        # Notifications of this very batch are already inserted but obviously
        # unacked, so ``unacked`` already includes them; compare against the
        # configured capacity directly.
        capacity = int(_meta_get(conn, 'capacity'))
        if unacked > capacity:
            raise SloError(
                'SLO notification capacity (%d) exceeded; batch rejected'
                % capacity)
    crash_hook('batch_commit')
    return {'enabled': True, 'notifications': notifications}


# --------------------------------------------------------------------------- #
# initialization
# --------------------------------------------------------------------------- #
def _read_catalog_claim(conn):
    return conn.execute(
        "SELECT value FROM meta WHERE key='retirement_catalog'").fetchone()


def slo_init(db_path, data):
    """Enable the SLO pipeline; idempotent on identical parsed config."""
    capacity, policies = validate_config(data)
    digest = config_digest(data)
    from .warehouse import connect, ensure_schema
    conn = connect(db_path)
    try:
        ensure_schema(conn)
        conn.execute('BEGIN IMMEDIATE')
        try:
            # Mutual exclusion with the retirement catalog, checked before any
            # binding or cleanup-style write happens.
            if _read_catalog_claim(conn) is not None:
                raise SloError(
                    'warehouse is bound to a retirement catalog; SLO is mutually exclusive')
            # DDL participates in the write transaction: a later rollback
            # removes these tables too, leaving no partial state behind.
            ensure_slo_schema(conn)
            stored_digest = _meta_get(conn, 'config_sha256')
            if stored_digest is not None:
                if stored_digest != digest:
                    raise SloError(
                        'SLO pipeline already initialized with different config')
                # Idempotent retry of the exact parsed content.
                conn.execute('COMMIT')
                return {'enabled': True, 'replayed': True,
                        'capacity': int(_meta_get(conn, 'capacity')),
                        'services': len(policies),
                        'notifications': int(_meta_get(conn, 'init_notifications')
                                             or 0)}
            _meta_set(conn, 'capacity', capacity)
            conn.executemany(
                'INSERT INTO slo_policies(service,slow_ticks,short_n,long_n,'
                'open_ppm,close_ppm) VALUES(?,?,?,?,?,?)',
                [(p['service'], p['slow_ticks'], p['short_n'], p['long_n'],
                  p['open_ppm'], p['close_ppm']) for p in policies])
            # Single streaming pass over the currently sealed set: one root
            # fact per sealed trace, inserted straight into the per-service
            # trees (no full-service materialization in Python).
            max_seq_row = conn.execute(
                "SELECT value FROM meta WHERE key='max_seq'").fetchone()
            init_seq = int(max_seq_row[0]) if max_seq_row is not None else 0
            trace_rows = conn.execute(
                "SELECT trace_id FROM traces WHERE status='sealed' "
                "ORDER BY trace_id").fetchall()
            policy_by_service = {p['service']: p for p in policies}
            for (trace_id,) in trace_rows:
                root = conn.execute(
                    'SELECT start,end,service FROM spans WHERE trace_id=? '
                    'AND parent IS NULL', (trace_id,)).fetchone()
                if root is not None and root[2] in policy_by_service:
                    start, end, service = root
                    policy = policy_by_service[service]
                    OrderTree(conn, service).insert(
                        end, trace_id, (end - start) >= policy['slow_ticks'])
            notifications = []
            for policy in policies:
                service = policy['service']
                notice = evaluate_service(conn, service, policy, init_seq)
                if notice is not None:
                    notifications.append(notice)
            if _unacked_count(conn) > capacity:
                raise SloError(
                    'SLO notification capacity (%d) exceeded at initialization'
                    % capacity)
            _meta_set(conn, 'config_sha256', digest)
            _meta_set(conn, 'init_notifications', len(notifications))
            _meta_set(conn, 'clock', 0)
            # The enabled marker is the last write inside the enabling
            # transaction: a killed/rolled-back init leaves no enabled flag,
            # no partial notifications and no policy rows behind.
            conn.execute(
                "INSERT INTO slo_meta(key,value) VALUES('enabled','1')")
            crash_hook('init_commit')
            conn.execute('COMMIT')
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    return {'enabled': True, 'replayed': False, 'capacity': capacity,
            'services': len(policies),
            'notifications': len(notifications)}


# --------------------------------------------------------------------------- #
# status snapshot
# --------------------------------------------------------------------------- #
def slo_status(db_path):
    from .warehouse import connect
    if not Path(db_path).exists():
        raise SloError('warehouse database does not exist')
    conn = connect(db_path)
    try:
        require_enabled(conn)
        conn.execute('BEGIN')
        try:
            max_seq = int(conn.execute(
                "SELECT value FROM meta WHERE key='max_seq'").fetchone()[0])
            services = []
            for service, state in conn.execute(
                    'SELECT service,state FROM slo_service_state ORDER BY service'):
                tree = OrderTree(conn, service)
                total, _slow = tree.total()
                services.append({'service': service, 'state': state,
                                 'requests': total})
            # Services without any state row are healthy by definition; the
            # report covers every policy service in UTF-8 byte order.
            listed = {s['service'] for s in services}
            for policy in conn.execute(
                    'SELECT service FROM slo_policies ORDER BY service'):
                service = policy[0]
                if service not in listed:
                    total = OrderTree(conn, service).total()[0]
                    services.append({'service': service, 'state': 'healthy',
                                     'requests': total})
            services.sort(key=lambda item: item['service'].encode('utf-8'))
            unacked = _unacked_count(conn)
            result = {'max_seq': max_seq, 'unacked': unacked,
                      'services': services}
            conn.execute('COMMIT')
            return result
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# claim / ack delivery
# --------------------------------------------------------------------------- #
def _advance_clock(conn, now):
    current = int(_meta_get(conn, 'clock') or 0)
    if now < current:
        raise SloError('delivery clock cannot move backwards (now=%d, held=%d)'
                       % (now, current))
    _meta_set(conn, 'clock', now)


def _new_token(event_id, now):
    # Non-empty opaque token; every (re)claim mints a fresh one.
    material = ('%d|%d|%s' % (
        event_id, now,
        hashlib.sha256(os.urandom(32)).hexdigest())).encode('utf-8')
    return 'tok-' + hashlib.sha256(material).hexdigest()[:32]


def slo_claim(db_path, data):
    consumer, now, lease_ticks, limit = validate_claim(data)
    from .warehouse import connect
    conn = connect(db_path)
    try:
        require_enabled(conn)
        conn.execute('BEGIN IMMEDIATE')
        try:
            _advance_clock(conn, now)
            expires_at = now + lease_ticks
            rows = conn.execute(
                'SELECT n.event_id,n.service,n.transition,n.max_seq,'
                'n.short_bad,n.short_total,n.long_bad,n.long_total '
                'FROM slo_notifications n WHERE NOT EXISTS ('
                'SELECT 1 FROM slo_ack_evidence a WHERE a.event_id=n.event_id) '
                'AND NOT EXISTS (SELECT 1 FROM slo_leases l '
                'WHERE l.event_id=n.event_id AND l.expires_at>?) '
                'ORDER BY n.event_id LIMIT ?',
                (now, limit)).fetchall()
            items = []
            for row in rows:
                event_id = row[0]
                token = _new_token(event_id, now)
                conn.execute(
                    'INSERT INTO slo_leases(event_id,consumer,token,expires_at) '
                    'VALUES(?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET '
                    'consumer=excluded.consumer,token=excluded.token,'
                    'expires_at=excluded.expires_at',
                    (event_id, consumer, token, expires_at))
                items.append({
                    'event_id': event_id,
                    'notification': {
                        'event_id': row[0], 'service': row[1],
                        'transition': row[2], 'max_seq': row[3],
                        'short_bad': row[4], 'short_total': row[5],
                        'long_bad': row[6], 'long_total': row[7]},
                    'token': token, 'expires_at': expires_at})
            crash_hook('claim_commit')
            conn.execute('COMMIT')
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    return {'consumer': consumer, 'now': now, 'items': items,
            'returned': len(items)}


def slo_ack(db_path, data):
    consumer, now, items = validate_ack(data)
    from .warehouse import connect
    conn = connect(db_path)
    try:
        require_enabled(conn)
        conn.execute('BEGIN IMMEDIATE')
        try:
            # Validate the whole ack atomically first: any illegal item fails
            # the entire request and nothing gets confirmed.
            plan = []
            for event_id, token in items:
                evidence = conn.execute(
                    'SELECT consumer,token FROM slo_ack_evidence WHERE event_id=?',
                    (event_id,)).fetchone()
                if evidence is not None:
                    # Idempotent only for the same consumer/token pair; any
                    # other token must not confirm an already-acked item.
                    if evidence[0] != consumer or evidence[1] != token:
                        raise SloError(
                            'event %d already acknowledged with another token'
                            % event_id)
                    plan.append((event_id, True))
                    continue
                lease = conn.execute(
                    'SELECT consumer,token,expires_at FROM slo_leases WHERE event_id=?',
                    (event_id,)).fetchone()
                if lease is None:
                    raise SloError('unknown or unclaimed event_id: %d' % event_id)
                lease_consumer, lease_token, expires_at = lease
                if lease_consumer != consumer or lease_token != token:
                    raise SloError(
                        'token/consumer mismatch for event_id: %d' % event_id)
                if not now < expires_at:
                    raise SloError(
                        'lease expired for event_id: %d' % event_id)
                plan.append((event_id, False))
            _advance_clock(conn, now)
            newly_acked = 0
            for event_id, already in plan:
                if already:
                    continue
                conn.execute(
                    'INSERT INTO slo_ack_evidence(event_id,consumer,token,'
                    'acked_at) VALUES(?,?,?,?)',
                    (event_id, consumer, token, now))
                conn.execute('DELETE FROM slo_leases WHERE event_id=?',
                             (event_id,))
                newly_acked += 1
            crash_hook('ack_commit')
            conn.execute('COMMIT')
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    return {'consumer': consumer, 'now': now, 'acked': len(items),
            'newly_acked': newly_acked}
