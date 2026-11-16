"""Subprocess fault-injection tests for SLO crash atomicity and recovery.

Every crash is delivered through the test-only ``TRACE_LATENCY_SLO_CRASH``
hook, which calls ``os._exit`` at a durable boundary (right before a
COMMIT).  A second process reopens the same SQLite file and must observe
either the complete pre-crash state or the complete post-crash state --
never a committed batch without its notifications, nor a ghost notification.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trace_latency import apply_batch, slo_init, slo_status, slo_claim, slo_ack

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'


def up(seq, tid, start=0, end=10, service='api'):
    return {'seq': seq, 'trace_id': tid, 'op': 'upsert',
            'span': {'id': 'r', 'parent': None, 'start': start,
                     'end': end, 'service': service}}


def seal(seq, tid):
    return {'seq': seq, 'trace_id': tid, 'op': 'seal'}


def batch(bid, events):
    return {'batch_id': bid, 'events': events}


POLICY = {'capacity': 100, 'policies': [{
    'service': 'api', 'slow_ticks': 10, 'short_n': 1, 'long_n': 1,
    'open_ppm': 1, 'close_ppm': 0}]}


class CrashRecovery(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.base = Path(self.td.name)
        self.db = self.base / 'w.db'
        self.helper = self.base / 'slo_crash_helper.py'
        self.helper.write_text(
            'import json, sys\n'
            'sys.path.insert(0, %r)\n'
            'from trace_latency import apply_batch, slo_init, slo_claim, '
            'slo_ack\n'
            'cmd = sys.argv[1]\n'
            'db = sys.argv[2]\n'
            'payload = json.load(open(sys.argv[3]))\n'
            'if cmd == "init":\n'
            '    print(json.dumps(slo_init(db, payload)))\n'
            'elif cmd == "apply":\n'
            '    print(json.dumps(apply_batch(db, payload)))\n'
            'elif cmd == "claim":\n'
            '    print(json.dumps(slo_claim(db, payload)))\n'
            'elif cmd == "ack":\n'
            '    print(json.dumps(slo_ack(db, payload)))\n'
            % str(SRC))

    def tearDown(self):
        self.td.cleanup()

    def run_helper(self, cmd, payload, crash=None):
        source = self.base / ('%s.json' % cmd)
        source.write_text(json.dumps(payload))
        env = dict(os.environ, PYTHONPATH=str(SRC))
        if crash is not None:
            env['TRACE_LATENCY_SLO_CRASH'] = crash
        return subprocess.run(
            [sys.executable, str(self.helper), cmd, str(self.db), str(source)],
            capture_output=True, text=True, env=env, cwd=ROOT)

    def assert_warehouse_intact(self, max_seq, notifications):
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT value FROM meta WHERE key='max_seq'").fetchone()[0],
            str(max_seq))
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_notifications').fetchone()[0],
            notifications)
        # every notification beyond the acked set has consistent lease/state
        rows = conn.execute(
            'SELECT event_id FROM slo_notifications ORDER BY event_id'
        ).fetchall()
        ids = [r[0] for r in rows]
        self.assertEqual(ids, list(range(1, notifications + 1)))
        conn.close()

    def test_crash_before_batch_commit_then_exact_retry(self):
        apply_batch(self.db, batch('b1', [up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(self.db, POLICY)
        # b2 flips healthy -> ... actually a slow request stays firing; craft
        # a close flip by revising t1 to fast.
        flip = batch('b2', [up(3, 't1', 0, 3), seal(4, 't1')])
        crashed = self.run_helper('apply', flip, crash='batch_commit')
        self.assertNotEqual(crashed.returncode, 0)
        # nothing from the killed transaction survived
        self.assert_warehouse_intact(max_seq=2, notifications=1)
        self.assertEqual(slo_status(self.db)['services'][0]['state'], 'firing')
        # exact retry in a new process commits once with the same batch id
        ok = self.run_helper('apply', flip)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        result = json.loads(ok.stdout)
        self.assertEqual((result['seq_from'], result['seq_to']), (3, 4))
        self.assert_warehouse_intact(max_seq=4, notifications=2)
        self.assertEqual(slo_status(self.db)['services'][0]['state'], 'healthy')
        # another exact retry is the idempotent replay and adds nothing
        replay = self.run_helper('apply', flip)
        self.assertTrue(json.loads(replay.stdout)['replayed'])
        self.assert_warehouse_intact(max_seq=4, notifications=2)

    def test_crash_before_init_commit_leaves_no_enabled_marker(self):
        apply_batch(self.db, batch('b1', [up(1, 't1', 0, 10), seal(2, 't1')]))
        crashed = self.run_helper('init', POLICY, crash='init_commit')
        self.assertNotEqual(crashed.returncode, 0)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'slo_%'").fetchone()[0], 0)
        conn.close()
        # warehouse is still usable through the ordinary entry
        ok = self.run_helper('init', POLICY)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertFalse(json.loads(ok.stdout)['replayed'])
        self.assertEqual(slo_status(self.db)['unacked'], 1)

    def test_crash_before_claim_commit_leaves_no_lease(self):
        apply_batch(self.db, batch('b1', [up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(self.db, POLICY)
        claim_req = {'consumer': 'A', 'now': 5, 'lease_ticks': 10, 'limit': 5}
        crashed = self.run_helper('claim', claim_req, crash='claim_commit')
        self.assertNotEqual(crashed.returncode, 0)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_leases').fetchone()[0], 0)
        # the clock did not advance either (whole transaction rolled back)
        self.assertEqual(conn.execute(
            "SELECT value FROM slo_meta WHERE key='slo_clock'").fetchone()[0],
            '0')
        conn.close()
        ok = self.run_helper('claim', claim_req)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertEqual(json.loads(ok.stdout)['returned'], 1)

    def test_crash_before_ack_commit_then_retry(self):
        apply_batch(self.db, batch('b1', [up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(self.db, POLICY)
        claim = slo_claim(self.db, {
            'consumer': 'A', 'now': 5, 'lease_ticks': 100, 'limit': 5})
        item = claim['items'][0]
        ack_req = {'consumer': 'A', 'now': 6, 'items': [
            {'event_id': item['event_id'], 'token': item['token']}]}
        crashed = self.run_helper('ack', ack_req, crash='ack_commit')
        self.assertNotEqual(crashed.returncode, 0)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_ack_evidence').fetchone()[0], 0)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_leases').fetchone()[0], 1)
        conn.close()
        # the notification still occupies capacity
        self.assertEqual(slo_status(self.db)['unacked'], 1)
        ok = self.run_helper('ack', ack_req)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertEqual(json.loads(ok.stdout)['newly_acked'], 1)
        self.assertEqual(slo_status(self.db)['unacked'], 0)


class CrossServiceAndRepeatedSeal(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def test_trace_moves_between_root_services_with_revision(self):
        cfg = {'capacity': 100, 'policies': [
            {'service': 'api', 'slow_ticks': 10, 'short_n': 1, 'long_n': 1,
             'open_ppm': 1, 'close_ppm': 0},
            {'service': 'db', 'slow_ticks': 10, 'short_n': 1, 'long_n': 1,
             'open_ppm': 1, 'close_ppm': 0}]}
        apply_batch(self.db, batch('b1', [
            up(1, 't1', 0, 10, 'api'), seal(2, 't1'),
            up(3, 't2', 0, 10, 'db'), seal(4, 't2')]))
        slo_init(self.db, cfg)
        # move t1 api -> db inside a batch that also reopens and reseals t2
        apply_batch(self.db, batch('b2', [
            up(5, 't1', 0, 10, 'db'), seal(6, 't1'),
            up(7, 't2', 0, 10, 'db'), seal(8, 't2')]))
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM slo_requests WHERE service='api'").fetchone()[0], 0)
        self.assertEqual(sorted(r[0] for r in conn.execute(
            "SELECT trace_id FROM slo_requests WHERE service='db'")),
            ['t1', 't2'])
        # each trace still contributes exactly one fact
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_requests').fetchone()[0], 2)
        conn.close()

    def test_repeated_seal_within_one_batch_ends_consistent(self):
        apply_batch(self.db, batch('b1', [up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(self.db, POLICY)
        # reopen, seal, reopen again and seal finally within one batch: only
        # the final sealed set is evaluated, once.
        apply_batch(self.db, batch('b2', [
            up(3, 't1', 0, 3), seal(4, 't1'),
            up(5, 't1', 0, 4), seal(6, 't1')]))
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM slo_requests').fetchone()[0], 1)
        self.assertEqual(conn.execute(
            'SELECT end_tick,slow FROM slo_requests').fetchone(), (4, 0))
        conn.close()
        notifications = sqlite3.connect(self.db).execute(
            'SELECT count(*) FROM slo_notifications').fetchone()[0]
        # one open at init, one close at the final fast state: no intra-batch
        # notifications.
        self.assertEqual(notifications, 2)

    def test_same_tick_lease_expiry_across_processes(self):
        apply_batch(self.db, batch('b1', [up(1, 't1', 0, 10), seal(2, 't1')]))
        slo_init(self.db, POLICY)
        first = slo_claim(self.db, {
            'consumer': 'A', 'now': 5, 'lease_ticks': 5, 'limit': 5})
        token_a = first['items'][0]['token']
        # B re-claims at the exact expiry tick in a fresh process; the new
        # lease invalidates A's token.
        helper = Path(self.td.name) / 'claim_helper.py'
        helper.write_text(
            'import json, sys\n'
            'sys.path.insert(0, %r)\n'
            'from trace_latency import slo_claim\n'
            'print(json.dumps(slo_claim(sys.argv[1], '
            'json.load(open(sys.argv[2])))))\n' % str(SRC))
        env = dict(os.environ, PYTHONPATH=str(SRC))
        source = Path(self.td.name) / 'claim.json'
        source.write_text(json.dumps(
            {'consumer': 'B', 'now': 10, 'lease_ticks': 5, 'limit': 5}))
        proc = subprocess.run(
            [sys.executable, str(helper), self.db, str(source)],
            capture_output=True, text=True, env=env, cwd=ROOT)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        token_b = json.loads(proc.stdout)['items'][0]['token']
        self.assertNotEqual(token_a, token_b)
        with self.assertRaises(Exception):
            slo_ack(self.db, {'consumer': 'A', 'now': 10, 'items': [
                {'event_id': 1, 'token': token_a}]})


class ScaleAndNoPolicy(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def test_many_out_of_window_requests_small_revision_is_bounded(self):
        # Tens of thousands of sealed root requests; long window 1000, short
        # window 100 -- far more history than either window covers.
        n = 40000
        cfg = {'capacity': 1000, 'policies': [{
            'service': 'api', 'slow_ticks': 10, 'short_n': 100,
            'long_n': 1000, 'open_ppm': 10**6, 'close_ppm': 0}]}
        events = []
        seq = 1
        for i in range(n):
            tid = 't%06d' % i
            events.append(up(seq, tid, 0, i + 1, 'api'))
            seq += 1
            events.append(seal(seq, tid))
            seq += 1
        for off in range(0, len(events), 5000):
            apply_batch(self.db, batch('bulk%d' % off,
                                       events[off:off + 5000]))
        slo_init(self.db, cfg)
        conn = sqlite3.connect(self.db)
        from trace_latency.slo import OrderTree
        tree = OrderTree(conn, 'api')
        # instrument SQL work for one small revision far outside the windows
        statements = []
        conn.set_trace_callback(lambda sql: statements.append(sql))
        tree.delete_fact('t000000')
        tree.insert(10**12, 't000000', False)
        conn.set_trace_callback(None)
        # O(log M) even for M=100k and large N: a few hundred statements,
        # never tens of thousands.
        self.assertLess(len(statements), 500)
        # window reads stay bounded too
        statements.clear()
        conn.set_trace_callback(lambda sql: statements.append(sql))
        tree.suffix_stats(1000)
        tree.suffix_stats(100)
        conn.set_trace_callback(None)
        self.assertLess(len(statements), 120)
        conn.close()

    def test_no_policy_service_is_ignored_but_still_warehoused(self):
        apply_batch(self.db, batch('b1', [
            up(1, 't1', 0, 10, 'api'), seal(2, 't1'),
            up(3, 't2', 0, 10, 'other'), seal(4, 't2')]))
        slo_init(self.db, POLICY)
        conn = sqlite3.connect(self.db)
        self.assertEqual(sorted(r[0] for r in conn.execute(
            'SELECT service FROM slo_requests')), ['api'])
        conn.close()
        # the unmanaged service still participates in occupancy analysis
        result = __import__(
            'trace_latency.warehouse', fromlist=['query']).query(
            self.db, {'service': 'other', 'start': 0, 'end': 20})
        self.assertEqual(result['peak'], 1)
        # revisions of the unmanaged trace never flip SLO state
        apply_batch(self.db, batch('b2', [
            up(5, 't2', 0, 99, 'other'), seal(6, 't2')]))
        self.assertEqual(slo_status(self.db)['max_seq'], 6)
        self.assertEqual(slo_status(self.db)['unacked'], 1)


if __name__ == '__main__':
    unittest.main()
