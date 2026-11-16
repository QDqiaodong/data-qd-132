"""Functional tests for the persistent, revision-aware SLO alert pipeline."""
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trace_latency import (
    apply_batch, slo_init, slo_status, slo_claim, slo_ack, SloError)
from trace_latency.slo import (
    OrderTree, ensure_slo_schema, validate_config, validate_claim,
    validate_ack, is_enabled)
from trace_latency.warehouse import query

ROOT = Path(__file__).resolve().parents[1]


def up(seq, tid, start=0, end=10, service='api', sid='r', parent=None):
    return {'seq': seq, 'trace_id': tid, 'op': 'upsert',
            'span': {'id': sid, 'parent': parent, 'start': start,
                     'end': end, 'service': service}}


def seal(seq, tid):
    return {'seq': seq, 'trace_id': tid, 'op': 'seal'}


def batch(bid, events):
    return {'batch_id': bid, 'events': events}


def root_span(tid, start, end, service):
    return up(0, tid, start, end, service)


def policy(service='api', **over):
    base = {'service': service, 'slow_ticks': 10, 'short_n': 2, 'long_n': 3,
            'open_ppm': 500000, 'close_ppm': 0}
    base.update(over)
    return base


class ConfigValidation(unittest.TestCase):
    def bad(self, data):
        with self.assertRaises((SloError, ValueError, TypeError)):
            validate_config(data)

    def test_shape_and_bounds(self):
        self.bad({})
        self.bad({'capacity': 1})
        self.bad({'capacity': 1, 'policies': [], 'x': 1})
        self.bad({'capacity': 0, 'policies': [policy()]})
        self.bad({'capacity': 10001, 'policies': [policy()]})
        self.bad({'capacity': True, 'policies': [policy()]})
        self.bad({'capacity': '1', 'policies': [policy()]})

    def test_policy_bounds(self):
        self.bad({'capacity': 1, 'policies': []})
        self.bad({'capacity': 1, 'policies': [
            policy('s%d' % i) for i in range(101)]})
        self.bad({'capacity': 1, 'policies': [policy(), policy()]})
        self.bad({'capacity': 1, 'policies': [policy(short_n=4, long_n=3)]})
        self.bad({'capacity': 1, 'policies': [policy(long_n=10**6 + 1)]})
        self.bad({'capacity': 1, 'policies': [policy(short_n=True)]})
        self.bad({'capacity': 1, 'policies': [policy(slow_ticks=10**12 + 1)]})
        self.bad({'capacity': 1, 'policies': [policy(open_ppm=10**6 + 1)]})
        self.bad({'capacity': 1, 'policies': [policy(close_ppm=500000)]})
        self.bad({'capacity': 1, 'policies': [policy(open_ppm=0)]})
        self.bad({'capacity': 1, 'policies': [
            {**policy(), 'service': ' leading-space'}]})
        self.bad({'capacity': 1, 'policies': [{
            'service': 's', 'slow_ticks': 0, 'short_n': 1, 'long_n': 1,
            'open_ppm': 1, 'close_ppm': True}]})

    def test_accepts_boundary_values(self):
        cap, policies = validate_config({
            'capacity': 10000,
            'policies': [policy(short_n=1, long_n=10**6,
                                open_ppm=10**6, close_ppm=10**6 - 1)]})
        self.assertEqual(cap, 10000)
        self.assertEqual(policies[0]['long_n'], 10**6)


class ClaimAckValidation(unittest.TestCase):
    def test_claim(self):
        for bad in (
            {'consumer': 'c', 'now': True, 'lease_ticks': 1, 'limit': 1},
            {'consumer': 'c', 'now': -1, 'lease_ticks': 1, 'limit': 1},
            {'consumer': 'c', 'now': 10**12, 'lease_ticks': 1, 'limit': 1},
            {'consumer': 'c', 'now': 0, 'lease_ticks': 0, 'limit': 1},
            {'consumer': 'c', 'now': 0, 'lease_ticks': 10**6 + 1, 'limit': 1},
            {'consumer': 'c', 'now': 0, 'lease_ticks': 1, 'limit': 0},
            {'consumer': 'c', 'now': 0, 'lease_ticks': 1, 'limit': 101},
            {'consumer': '', 'now': 0, 'lease_ticks': 1, 'limit': 1},
            {'consumer': 'c', 'now': 0, 'lease_ticks': 1},
        ):
            with self.assertRaises((SloError, ValueError)):
                validate_claim(bad)
        self.assertEqual(
            validate_claim({'consumer': 'c', 'now': 10**12 - 1,
                            'lease_ticks': 1, 'limit': 1})[3], 1)

    def test_ack(self):
        for bad in (
            {'consumer': 'c', 'now': 0, 'items': []},
            {'consumer': 'c', 'now': 0,
             'items': [{'event_id': i, 'token': 't'} for i in range(101)]},
            {'consumer': 'c', 'now': 0, 'items': [
                {'event_id': 1, 'token': 'a'}, {'event_id': 1, 'token': 'b'}]},
            {'consumer': 'c', 'now': 0,
             'items': [{'event_id': True, 'token': 'a'}]},
            {'consumer': 'c', 'now': 0,
             'items': [{'event_id': 1, 'token': ''}]},
        ):
            with self.assertRaises((SloError, ValueError)):
                validate_ack(bad)


class AvlOrderTree(unittest.TestCase):
    """AVL order-statistics tree vs. a brute-force sorted-list model."""

    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        ensure_slo_schema(self.conn)
        self.tree = OrderTree(self.conn, 'svc')

    def tearDown(self):
        self.conn.close()

    def _check(self):
        def rec(node_id, depth=0):
            if node_id is None:
                return 0, 0, 0
            r = self.tree._row(node_id)
            if r[2] is not None:
                self.assertEqual(self.tree._row(r[2])[1], node_id)
            if r[3] is not None:
                self.assertEqual(self.tree._row(r[3])[1], node_id)
            lh, lsz, lsl = rec(r[2], depth + 1)
            rh, rsz, rsl = rec(r[3], depth + 1)
            self.assertLessEqual(abs(lh - rh), 1)
            self.assertEqual(r[4], 1 + max(lh, rh))
            self.assertEqual(r[5], 1 + lsz + rsz)
            self.assertEqual(r[6], r[10] + lsl + rsl)
            return r[4], r[5], r[6]
        root = self.tree._root_id()
        if root is None:
            return
        self.assertIsNone(self.tree._row(root)[1])
        rec(root)

    def test_random_insert_delete_matches_sorted_model(self):
        import random
        random.seed(1234)
        live = {}
        for _ in range(4000):
            tid = 'k%05d' % random.randint(0, 600)
            if tid in live:
                self.tree.delete_fact(tid)
                del live[tid]
            else:
                end = random.randint(0, 8)
                slow = random.random() < 0.4
                self.tree.insert(end, tid, slow)
                live[tid] = (end, tid, slow)
            want = sorted(live.values(),
                          key=lambda e: (e[0], e[1].encode()))
            self.assertEqual(self.tree.ordered_trace_ids(), want)
            self._check()
            for n in (1, 3, 50):
                w = want[-min(n, len(want)):]
                self.assertEqual(
                    self.tree.suffix_stats(n),
                    (len(w), sum(1 for e in w if e[2])))

    def test_same_tick_deterministic_byte_order_and_exact_n(self):
        for tid, slow in (('a', True), ('b', False), ('c', True)):
            self.tree.insert(10, tid, slow)
        self.assertEqual(
            [r[1] for r in self.tree.ordered_trace_ids()], ['a', 'b', 'c'])
        # N=2 keeps exactly b,c (the whole same-tick group is not kept).
        self.assertEqual(self.tree.suffix_stats(2), (2, 1))
        self.assertEqual(self.tree.suffix_stats(1), (1, 1))
        self.assertEqual(self.tree.suffix_stats(3), (3, 2))

    def test_large_tree_height_is_logarithmic(self):
        n = 20000
        for i in range(n):
            self.tree.insert(7, 'z%06d' % i, i % 3 == 0)
        self._check()
        height = self.tree._row(self.tree._root_id())[4]
        self.assertLess(height, 1.45 * (n.bit_length()) + 2)
        self.assertEqual(self.tree.total(), (n, sum(1 for i in range(n)
                                                     if i % 3 == 0)))


class SloFlowBase(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def seed_requests(self, specs, bid_prefix='b'):
        """specs: list of (trace_id, start, end, service)."""
        events = []
        seq = 1
        for i, (tid, start, end, service) in enumerate(specs):
            events.append(up(seq, tid, start, end, service))
            seq += 1
            events.append(seal(seq, tid))
            seq += 1
        apply_batch(self.db, batch(bid_prefix, events))
        return seq - 1


class Initialization(SloFlowBase):
    def test_init_on_sealed_warehouse_produces_initial_open(self):
        self.seed_requests([
            ('t1', 0, 10, 'api'), ('t2', 0, 5, 'api'),
            ('t3', 0, 10, 'api')])
        result = slo_init(self.db, {'capacity': 10, 'policies': [policy()]})
        self.assertFalse(result['replayed'])
        self.assertEqual(result['notifications'], 1)
        self.assertTrue(is_enabled(sqlite3.connect(self.db)))
        notifications = sqlite3.connect(self.db).execute(
            'SELECT event_id,transition,short_bad,short_total,long_bad,'
            'long_total,max_seq FROM slo_notifications').fetchall()
        self.assertEqual(notifications, [(1, 'open', 2, 2, 2, 3, 6)])

    def test_init_idempotent_same_config_rejected_when_different(self):
        self.seed_requests([('t1', 0, 10, 'api')])
        cfg = {'capacity': 10, 'policies': [policy()]}
        first = slo_init(self.db, cfg)
        again = slo_init(self.db, json.loads(json.dumps(cfg)))
        self.assertTrue(again['replayed'])
        self.assertEqual(again['notifications'], first['notifications'])
        with self.assertRaises(SloError):
            slo_init(self.db, {'capacity': 9, 'policies': [policy()]})
        with self.assertRaises(SloError):
            slo_init(self.db, {
                'capacity': 10,
                'policies': [policy(open_ppm=600000)]})

    def test_init_failure_leaves_no_enabled_marker_or_partial_state(self):
        self.seed_requests([('t1', 0, 10, 'api')])
        # capacity 0 is rejected before the enabled marker is written.
        with self.assertRaises(SloError):
            slo_init(self.db, {'capacity': 0, 'policies': [policy()]})
        conn = sqlite3.connect(self.db)
        self.assertEqual(
            conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'slo_%'").fetchone()[0], 0)
        conn.close()
        # a later valid init still works cleanly
        ok = slo_init(self.db, {'capacity': 10, 'policies': [policy()]})
        self.assertFalse(ok['replayed'])

    def test_ops_without_init_fail(self):
        self.seed_requests([('t1', 0, 10, 'api')])
        with self.assertRaises(SloError):
            slo_status(self.db)
        with self.assertRaises(SloError):
            slo_claim(self.db, {'consumer': 'c', 'now': 1,
                                'lease_ticks': 1, 'limit': 1})
        with self.assertRaises(SloError):
            slo_ack(self.db, {'consumer': 'c', 'now': 1,
                              'items': [{'event_id': 1, 'token': 't'}]})


class WindowSemantics(SloFlowBase):
    def setUp(self):
        super().setUp()
        self.seed_requests([('t1', 0, 10, 'api')])
        slo_init(self.db, {'capacity': 100, 'policies': [policy(
            short_n=2, long_n=3, open_ppm=500000, close_ppm=0)]})

    def test_revision_moves_boundary_and_keeps_exact_n(self):
        # two more sealed requests, ends 20 and 30
        apply_batch(self.db, batch('b2', [
            up(3, 't2', 0, 20), seal(4, 't2'),
            up(5, 't3', 0, 30), seal(6, 't3')]))
        conn = sqlite3.connect(self.db)
        tree = OrderTree(conn, 'api')
        self.assertEqual(
            [r[1] for r in tree.ordered_trace_ids()],
            ['t1', 't2', 't3'])
        # make the oldest request (t1) the newest
        apply_batch(self.db, batch('b3', [
            up(7, 't1', 0, 40), seal(8, 't1')]))
        self.assertEqual(
            [r[1] for r in tree.ordered_trace_ids()],
            ['t2', 't3', 't1'])
        # the old boundary request t2 is now the first; the suffix of 2 is
        # exactly (t3, t1) -- never a whole same-tick group expansion.
        self.assertEqual(tree.suffix_stats(2)[0], 2)
        conn.close()

    def test_subspan_only_change_keeps_root_fact(self):
        def child(seq, start, end):
            return up(seq, 't1', start, end, 'api', sid='c', parent='r')
        apply_batch(self.db, batch('b2', [child(3, 2, 8), seal(4, 't1')]))
        conn = sqlite3.connect(self.db)
        before = conn.execute(
            'SELECT end_tick,slow,trace_id FROM slo_requests').fetchall()
        apply_batch(self.db, batch('b3', [child(5, 3, 7), seal(6, 't1')]))
        after = conn.execute(
            'SELECT end_tick,slow,trace_id FROM slo_requests').fetchall()
        self.assertEqual(before, after)
        conn.close()

    def test_cross_service_move_revokes_and_reinserts(self):
        cfg = {'capacity': 100, 'policies': [
            policy('api'), policy('db', short_n=1, long_n=1)]}
        db = str(Path(self.td.name) / 'multi.db')
        apply_batch(db, batch('b1', [
            up(1, 't1', 0, 10, 'api'), seal(2, 't1'),
            up(3, 't2', 0, 10, 'db'), seal(4, 't2')]))
        slo_init(db, cfg)
        apply_batch(db, batch('b2', [
            up(5, 't1', 0, 10, 'db'), seal(6, 't1')]))
        conn = sqlite3.connect(db)
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM slo_requests WHERE service='api'").fetchone()[0], 0)
        self.assertEqual(sorted(r[0] for r in conn.execute(
            "SELECT trace_id FROM slo_requests WHERE service='db'")),
            ['t1', 't2'])
        conn.close()

    def test_reopen_without_reseal_removes_fact(self):
        apply_batch(self.db, batch('b2', [up(3, 't1', 0, 3)]))  # reopen draft
        conn = sqlite3.connect(self.db)
        self.assertEqual(
            conn.execute('SELECT count(*) FROM slo_requests').fetchone()[0], 0)
        conn.close()
        # reseal re-enters the fact at its new position
        apply_batch(self.db, batch('b3', [seal(4, 't1')]))
        conn = sqlite3.connect(self.db)
        self.assertEqual(
            conn.execute('SELECT count(*) FROM slo_requests').fetchone()[0], 1)
        conn.close()


class AlertTransitions(SloFlowBase):
    def test_open_then_quiet_then_close_lifecycle(self):
        self.seed_requests([
            ('t1', 0, 10, 'api'), ('t2', 0, 5, 'api'),
            ('t3', 0, 10, 'api')])
        slo_init(self.db, {'capacity': 100, 'policies': [policy()]})
        self.assertEqual(slo_status(self.db)['services'][0]['state'], 'firing')
        # an extra fast request keeps it firing without a new notification
        apply_batch(self.db, batch('b2', [
            up(7, 't4', 0, 4), seal(8, 't4')]))
        conn = sqlite3.connect(self.db)
        self.assertEqual(
            conn.execute('SELECT count(*) FROM slo_notifications').fetchone()[0], 1)
        conn.close()
        self.assertEqual(slo_status(self.db)['services'][0]['state'], 'firing')
        # revise every slow request inside the windows to fast -> one close
        apply_batch(self.db, batch('b3', [
            up(9, 't1', 0, 3), seal(10, 't1'),
            up(11, 't3', 0, 3), seal(12, 't3')]))
        self.assertEqual(slo_status(self.db)['services'][0]['state'], 'healthy')
        conn = sqlite3.connect(self.db)
        rows = conn.execute(
            'SELECT event_id,transition,short_bad,short_total FROM '
            'slo_notifications ORDER BY event_id').fetchall()
        self.assertEqual([r[1] for r in rows], ['open', 'close'])
        # the stored open counts are immutable
        self.assertEqual(rows[0], (1, 'open', 2, 2))
        conn.close()

    def test_event_ids_continuous_and_failed_batch_does_not_consume(self):
        self.seed_requests([('t1', 0, 10, 'api')])
        slo_init(self.db, {'capacity': 100, 'policies': [policy(
            short_n=1, long_n=1, open_ppm=1, close_ppm=0)]})
        # a failing batch (bad seal) must not consume an event id
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('bad', [
                up(3, 'ghost', 0, 10, 'api', sid='x', parent='missing'),
                seal(4, 'ghost')]))
        self.assertEqual(sqlite3.connect(self.db).execute(
            'SELECT MAX(event_id) FROM slo_notifications').fetchone()[0], 1)
        # the next successful flip continues the continuous event id sequence
        apply_batch(self.db, batch('b2', [up(3, 't1', 0, 3), seal(4, 't1')]))
        ids = [r[0] for r in sqlite3.connect(self.db).execute(
            'SELECT event_id FROM slo_notifications ORDER BY event_id')]
        self.assertEqual(ids, [1, 2])


class CapacityGate(SloFlowBase):
    def setUp(self):
        super().setUp()
        self.cfg = {'capacity': 1, 'policies': [policy(
            short_n=1, long_n=1, open_ppm=1, close_ppm=0)]}
        apply_batch(self.db, batch('b1', [
            up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(self.db, self.cfg)

    def test_close_batch_fully_rolls_back_then_retry_succeeds(self):
        with self.assertRaises(SloError):
            apply_batch(self.db, batch('b2', [
                up(3, 't1', 0, 3), seal(4, 't1')]))
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT value FROM meta WHERE key='max_seq'").fetchone()[0], '2')
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM batches').fetchone()[0], 1)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_notifications').fetchone()[0], 1)
        self.assertEqual(conn.execute('SELECT end FROM spans').fetchone()[0], 10)
        self.assertEqual(conn.execute(
            "SELECT status FROM traces").fetchone()[0], 'sealed')
        self.assertEqual(conn.execute(
            'SELECT slow FROM slo_requests').fetchone()[0], 1)
        conn.close()
        # claiming does not free capacity
        claim = slo_claim(self.db, {
            'consumer': 'A', 'now': 5, 'lease_ticks': 1000, 'limit': 5})
        self.assertEqual(claim['returned'], 1)
        with self.assertRaises(SloError):
            apply_batch(self.db, batch('b2', [
                up(3, 't1', 0, 3), seal(4, 't1')]))
        # ack frees the slot; the exact original batch_id retries once
        item = claim['items'][0]
        slo_ack(self.db, {'consumer': 'A', 'now': 6, 'items': [
            {'event_id': item['event_id'], 'token': item['token']}]})
        result = apply_batch(self.db, batch('b2', [
            up(3, 't1', 0, 3), seal(4, 't1')]))
        self.assertEqual((result['seq_from'], result['seq_to']), (3, 4))
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            'SELECT transition FROM slo_notifications ORDER BY event_id'
        ).fetchall(), [('open',), ('close',)])
        conn.close()

    def test_no_flip_batch_commits_while_full(self):
        result = apply_batch(self.db, batch('fast', [
            up(3, 't2', 0, 3), seal(4, 't2')]))
        self.assertEqual(result['max_seq'], 4)

    def test_two_service_flip_needs_two_slots(self):
        db = str(Path(self.td.name) / 'two.db')
        cfg = {'capacity': 1, 'policies': [
            policy('api', short_n=1, long_n=1, open_ppm=1, close_ppm=0),
            policy('db', short_n=1, long_n=1, open_ppm=1, close_ppm=0)]}
        apply_batch(db, batch('b1', [
            up(1, 'a', 0, 10, 'api'), seal(2, 'a')]))
        slo_init(db, cfg)   # one open fills the slot
        with self.assertRaises(SloError):
            apply_batch(db, batch('b2', [
                up(3, 'a', 0, 3, 'api'), seal(4, 'a'),     # api closes
                up(5, 'd', 0, 10, 'db'), seal(6, 'd')]))   # db opens
        conn = sqlite3.connect(db)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_notifications').fetchone()[0], 1)
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM slo_requests WHERE service='db'").fetchone()[0], 0)
        conn.close()


class Delivery(SloFlowBase):
    def setUp(self):
        super().setUp()
        apply_batch(self.db, batch('b1', [
            up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(self.db, {'capacity': 10, 'policies': [policy(
            short_n=1, long_n=1, open_ppm=1, close_ppm=0)]})

    def test_lease_expiry_token_rotation_and_idempotent_ack(self):
        first = slo_claim(self.db, {
            'consumer': 'A', 'now': 5, 'lease_ticks': 5, 'limit': 10})
        self.assertEqual(first['items'][0]['expires_at'], 10)
        event_id = first['items'][0]['event_id']
        token = first['items'][0]['token']
        # still leased at now=9
        self.assertEqual(slo_claim(self.db, {
            'consumer': 'B', 'now': 9, 'lease_ticks': 1,
            'limit': 10})['returned'], 0)
        # lease available again exactly at now=10 with a fresh token
        second = slo_claim(self.db, {
            'consumer': 'B', 'now': 10, 'lease_ticks': 5, 'limit': 10})
        self.assertEqual(second['returned'], 1)
        self.assertNotEqual(second['items'][0]['token'], token)
        new_token = second['items'][0]['token']
        # the old token/consumer can no longer ack
        with self.assertRaises(SloError):
            slo_ack(self.db, {'consumer': 'A', 'now': 10, 'items': [
                {'event_id': event_id, 'token': token}]})
        with self.assertRaises(SloError):
            slo_ack(self.db, {'consumer': 'A', 'now': 11, 'items': [
                {'event_id': event_id, 'token': new_token}]})
        # valid ack, then idempotent repeat even after expiry
        ack = slo_ack(self.db, {'consumer': 'B', 'now': 10, 'items': [
            {'event_id': event_id, 'token': new_token}]})
        self.assertEqual(ack['newly_acked'], 1)
        again = slo_ack(self.db, {'consumer': 'B', 'now': 999, 'items': [
            {'event_id': event_id, 'token': new_token}]})
        self.assertEqual(again['newly_acked'], 0)
        # a different token must not confirm the acked item
        with self.assertRaises(SloError):
            slo_ack(self.db, {'consumer': 'B', 'now': 1000, 'items': [
                {'event_id': event_id, 'token': 'tok-other'}]})

    def test_clock_monotonic_and_empty_claim_advances(self):
        slo_claim(self.db, {'consumer': 'A', 'now': 7,
                            'lease_ticks': 1, 'limit': 5})
        # same tick allowed
        slo_claim(self.db, {'consumer': 'A', 'now': 7,
                            'lease_ticks': 1, 'limit': 5})
        with self.assertRaises(SloError):
            slo_claim(self.db, {'consumer': 'A', 'now': 6,
                                'lease_ticks': 1, 'limit': 5})
        # ack the only notification then an empty claim still advances clock
        item = slo_claim(self.db, {
            'consumer': 'A', 'now': 8, 'lease_ticks': 100,
            'limit': 5})['items'][0]
        slo_ack(self.db, {'consumer': 'A', 'now': 9, 'items': [
            {'event_id': item['event_id'], 'token': item['token']}]})
        empty = slo_claim(self.db, {'consumer': 'A', 'now': 12,
                                    'lease_ticks': 1, 'limit': 5})
        self.assertEqual(empty['returned'], 0)
        with self.assertRaises(SloError):
            slo_claim(self.db, {'consumer': 'A', 'now': 11,
                                'lease_ticks': 1, 'limit': 5})

    def test_multi_item_ack_is_all_or_nothing(self):
        # two notifications on a fresh db
        db = str(Path(self.td.name) / 'multi.db')
        cfg = {'capacity': 100, 'policies': [policy(
            short_n=1, long_n=1, open_ppm=1, close_ppm=0)]}
        apply_batch(db, batch('b1', [
            up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(db, cfg)
        apply_batch(db, batch('b2', [up(3, 't1', 0, 3), seal(4, 't1')]))
        apply_batch(db, batch('b3', [up(5, 't1', 0, 10), seal(6, 't1')]))
        # open -> close -> open yields three notifications
        claim = slo_claim(db, {'consumer': 'C', 'now': 1,
                               'lease_ticks': 100, 'limit': 10})
        self.assertEqual(claim['returned'], 3)
        items = [{'event_id': i['event_id'], 'token': i['token']}
                 for i in claim['items']]
        items[1]['token'] = 'tok-wrong'
        with self.assertRaises(SloError):
            slo_ack(db, {'consumer': 'C', 'now': 2, 'items': items})
        conn = sqlite3.connect(db)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_ack_evidence').fetchone()[0], 0)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_leases').fetchone()[0], 3)
        conn.close()

    def test_unknown_event_id_fails_whole_ack(self):
        with self.assertRaises(SloError):
            slo_ack(self.db, {'consumer': 'A', 'now': 1, 'items': [
                {'event_id': 999, 'token': 'tok-x'}]})


class StatusSnapshot(SloFlowBase):
    def test_single_snapshot_shape_and_byte_order(self):
        cfg = {'capacity': 10, 'policies': [
            policy('zeta', short_n=1, long_n=1, open_ppm=1),
            policy('alpha', short_n=1, long_n=1, open_ppm=1)]}
        apply_batch(self.db, batch('b1', [
            up(1, 'z', 0, 10, 'zeta'), seal(2, 'z'),
            up(3, 'a', 0, 10, 'alpha'), seal(4, 'a')]))
        slo_init(self.db, cfg)
        snapshot = slo_status(self.db)
        self.assertEqual(snapshot['max_seq'], 4)
        self.assertEqual([s['service'] for s in snapshot['services']],
                         ['alpha', 'zeta'])
        self.assertTrue(all('state' in s and 'requests' in s
                            for s in snapshot['services']))
        self.assertEqual(snapshot['unacked'], 2)


class NonLinearWindowAccess(SloFlowBase):
    """A small revision must not scan the window or the full service."""

    def test_suffix_work_independent_of_window_size(self):
        cfg = {'capacity': 1000, 'policies': [policy(
            short_n=5000, long_n=10000, open_ppm=900000, close_ppm=0)]}
        # 12000 sealed root requests, mostly outside the long window after
        # later inserts land at higher end ticks.
        events = []
        seq = 1
        m = 12000
        for i in range(m):
            tid = 't%06d' % i
            events.append(up(seq, tid, 0, i, 'api'))
            seq += 1
            events.append(seal(seq, tid))
            seq += 1
        for off in range(0, len(events), 5000):
            apply_batch(self.db, batch('bulk%d' % off, events[off:off + 5000]))
        slo_init(self.db, cfg)
        conn = sqlite3.connect(self.db)
        tree = OrderTree(conn, 'api')
        statements = []
        conn.set_trace_callback(lambda sql: statements.append(sql))
        tree.suffix_stats(10000)
        tree.suffix_stats(5000)
        conn.set_trace_callback(None)
        # O(height) statements: far below the window size (thousands) and M.
        self.assertLess(len(statements), 200)
        # a tiny revision touches only O(log M) nodes
        statements.clear()
        conn.set_trace_callback(lambda sql: statements.append(sql))
        tree.delete_fact('t000001')
        tree.insert(10**12, 't000001', True)
        conn.set_trace_callback(None)
        self.assertLess(len(statements), 400)
        conn.close()


class Cli(unittest.TestCase):
    def call(self, *args):
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', *args],
            capture_output=True, text=True,
            cwd=ROOT, env={**__import__('os').environ,
                           'PYTHONPATH': str(ROOT / 'src')})

    def write(self, td, name, data):
        path = Path(td) / name
        path.write_text(json.dumps(data), encoding='utf-8')
        return path

    def test_cli_roundtrip_and_exit_2(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / 'w.db')
            self.write(td, 'batch.json', batch('b1', [
                up(1, 't1', 0, 10), seal(2, 't1')]))
            r = self.call('warehouse-apply', '--db', db,
                          '--source', str(Path(td) / 'batch.json'))
            self.assertEqual(r.returncode, 0, r.stderr)
            cfg = self.write(td, 'cfg.json', {
                'capacity': 5, 'policies': [policy(
                    short_n=1, long_n=1, open_ppm=1, close_ppm=0)]})
            r = self.call('slo-init', '--db', db, '--source', str(cfg))
            self.assertEqual(r.returncode, 0, r.stderr)
            report = json.loads(r.stdout)
            self.assertEqual(report['kind'], 'slo_init')
            r = self.call('slo-status', '--db', db)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertEqual(json.loads(r.stdout)['result']['unacked'], 1)
            claim_src = self.write(td, 'claim.json', {
                'consumer': 'A', 'now': 1, 'lease_ticks': 10, 'limit': 5})
            r = self.call('slo-claim', '--db', db, '--source', str(claim_src))
            self.assertEqual(r.returncode, 0, r.stderr)
            item = json.loads(r.stdout)['result']['items'][0]
            ack_src = self.write(td, 'ack.json', {'consumer': 'A', 'now': 2,
                                                  'items': [
                {'event_id': item['event_id'], 'token': item['token']}]})
            r = self.call('slo-ack', '--db', db, '--source', str(ack_src))
            self.assertEqual(r.returncode, 0, r.stderr)
            # strict JSON: duplicate keys rejected, exit 2
            bad = Path(td) / 'bad.json'
            bad.write_text('{"capacity":1,"capacity":2,"policies":[]}')
            r = self.call('slo-init', '--db', db, '--source', str(bad))
            self.assertEqual(r.returncode, 2)
            # unknown field rejected, exit 2
            bad2 = self.write(td, 'bad2.json', {'consumer': 'A', 'now': 1,
                                                'lease_ticks': 1, 'limit': 1,
                                                'x': 0})
            r = self.call('slo-claim', '--db', db, '--source', str(bad2))
            self.assertEqual(r.returncode, 2)


if __name__ == '__main__':
    unittest.main()
