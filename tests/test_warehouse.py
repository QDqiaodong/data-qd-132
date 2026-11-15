"""Tests for the offline SQLite warehouse and its service/time index."""
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from trace_latency import warehouse
from trace_latency.warehouse import apply_batch, query

ROOT = Path(__file__).resolve().parents[1]


def span(sid, parent, start, end, service):
    return {'id': sid, 'parent': parent, 'start': start, 'end': end,
            'service': service}


def up(seq, trace_id, sp):
    return {'seq': seq, 'trace_id': trace_id, 'op': 'upsert', 'span': sp}


def dele(seq, trace_id, sid):
    return {'seq': seq, 'trace_id': trace_id, 'op': 'delete', 'id': sid}


def seal(seq, trace_id):
    return {'seq': seq, 'trace_id': trace_id, 'op': 'seal'}


def batch(batch_id, events):
    return {'batch_id': batch_id, 'events': events}


class Validation(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def bad(self, data):
        with self.assertRaises((ValueError, TypeError)):
            apply_batch(self.db, data)

    def test_top_level_shape(self):
        self.bad({'events': []})
        self.bad({'batch_id': 'b', 'events': {}})
        self.bad({'batch_id': 'b', 'events': []})
        self.bad({'batch_id': 'b', 'events': [], 'extra': 1})

    def test_event_keys_per_op(self):
        base = span('r', None, 0, 1, 'api')
        self.bad(batch('b', [{'seq': 1, 'trace_id': 't', 'op': 'upsert'}]))
        self.bad(batch('b', [{'seq': 1, 'trace_id': 't', 'op': 'upsert',
                              'span': base, 'x': 1}]))
        self.bad(batch('b', [{'seq': 1, 'trace_id': 't', 'op': 'delete'}]))
        self.bad(batch('b', [dele(1, 't', 'r')]))  # span absent
        self.bad(batch('b', [{'seq': 1, 'trace_id': 't', 'op': 'seal',
                              'id': 'r'}]))
        self.bad(batch('b', [{'seq': 1, 'trace_id': 't', 'op': 'frobnicate'}]))

    def test_seq_and_time_reject_bool(self):
        for bad_seq in (True, False, 0, -1, '1', 1.0):
            self.bad(batch('b', [{'seq': bad_seq, 'trace_id': 't',
                                  'op': 'seal'}]))
        self.bad(batch('b', [up(1, 't', span('r', None, True, 1, 'api'))]))
        self.bad(batch('b', [up(1, 't', span('r', None, 0, False, 'api'))]))
        self.bad(batch('b', [up(1, 't', span('r', None, -1, 1, 'api'))]))
        self.bad(batch('b', [up(1, 't', span('r', None, 0, 10**12 + 1, 'api'))]))

    def test_ident_rules_and_reversed_interval(self):
        self.bad(batch('b', [up(1, 't', span(' r', None, 0, 1, 'api'))]))
        self.bad(batch('b', [up(1, 't', span('', None, 0, 1, 'api'))]))
        self.bad(batch('b', [up(1, 't', span('r', None, 2, 1, 'api'))]))

    def test_batch_size_limit(self):
        events = [up(i + 1, 't', span('s%05d' % i, None, 0, 0, 'api'))
                  for i in range(5001)]
        self.bad(batch('big', events))
        ok = apply_batch(self.db, batch('big', events[:5000]))
        self.assertEqual((ok['seq_from'], ok['seq_to']), (1, 5000))

    def test_span_limit_per_trace(self):
        # 4999 rootless zero-length spans plus one root: reaches the 5000 cap
        # without breaking the single-root rule on later seals.
        events = [up(1, 't', span('root', None, 0, 0, 'api'))]
        events += [up(i + 2, 't', span('s%05d' % i, 'root', 0, 0, 'api'))
                   for i in range(4999)]
        self.assertEqual(len(events), 5000)
        apply_batch(self.db, batch('fill', events))
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('overflow', [
                up(5001, 't', span('more', 'root', 0, 0, 'api'))]))

    def test_seal_rules_and_draft_freedom(self):
        # drafts allow missing parent, cycles and child intervals outside parent
        draft = batch('d', [
            up(1, 't', span('c', 'missing', 5, 9, 'api')),
            up(2, 't', span('r', None, 0, 4, 'api')),
            up(3, 't', span('x', 'x', 0, 0, 'api')),
        ])
        apply_batch(self.db, draft)
        self.bad(batch('s1', [seal(4, 't')]))           # invalid graph
        self.bad(batch('s2', [seal(4, 'ghost')]))        # no spans
        apply_batch(self.db, batch('fix', [dele(4, 't', 'x'), dele(5, 't', 'c')]))
        apply_batch(self.db, batch('ok', [seal(6, 't')]))
        self.bad(batch('again', [seal(7, 't')]))        # already sealed

    def test_query_shape(self):
        with self.assertRaises(ValueError):
            query(self.db, {'service': 'api', 'start': 5, 'end': 5})
        with self.assertRaises(ValueError):
            query(self.db, {'service': 'api', 'start': True, 'end': 1})
        with self.assertRaises(ValueError):
            query(self.db, {'service': 'api', 'start': 0, 'end': 1, 'x': 1})


class SequenceAndIdempotency(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def test_global_continuous_seq(self):
        apply_batch(self.db, batch('b1', [
            up(1, 't', span('r', None, 0, 1, 'api')),
            up(2, 't', span('a', 'r', 0, 1, 'api')),
        ]))
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('b2', [up(4, 't', span('b', 'r', 0, 1, 'api'))]))
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('b2', [up(2, 't', span('b', 'r', 0, 1, 'api'))]))
        # failed batch did not consume seq 3
        apply_batch(self.db, batch('b2', [up(3, 't', span('b', 'r', 0, 1, 'api'))]))
        self.assertEqual(query(self.db, {'service': 'api', 'start': 0, 'end': 1})['max_seq'], 3)

    def test_replay_returns_original_seq_and_writes_nothing(self):
        data = batch('b1', [up(1, 't', span('r', None, 0, 1, 'api'))])
        first = apply_batch(self.db, data)
        conn = sqlite3.connect(self.db)
        version_before = conn.execute(
            "SELECT version FROM traces WHERE trace_id='t'").fetchone()[0]
        conn.close()
        replayed = apply_batch(self.db, json.loads(json.dumps(data)))
        self.assertTrue(replayed['replayed'])
        self.assertEqual((replayed['seq_from'], replayed['seq_to']),
                         (first['seq_from'], first['seq_to']))
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute('SELECT count(*) FROM batches').fetchone()[0], 1)
        self.assertEqual(conn.execute('SELECT count(*) FROM spans').fetchone()[0], 1)
        # exact retry must not create a new revision
        self.assertEqual(conn.execute(
            "SELECT version FROM traces WHERE trace_id='t'").fetchone()[0],
            version_before)
        conn.close()

    def test_key_order_independence(self):
        data = batch('b1', [up(1, 't', span('r', None, 0, 1, 'api'))])
        apply_batch(self.db, data)
        reordered = json.loads('{"events":[{"span":{"service":"api","end":1,"start":0,"parent":null,"id":"r"},"op":"upsert","trace_id":"t","seq":1}],"batch_id":"b1"}')
        result = apply_batch(self.db, reordered)
        self.assertTrue(result['replayed'])

    def test_changed_content_rejected_and_writes_nothing(self):
        apply_batch(self.db, batch('b1', [up(1, 't', span('r', None, 0, 1, 'api'))]))
        changed = batch('b1', [up(1, 't', span('r', None, 0, 2, 'api'))])
        with self.assertRaises(ValueError):
            apply_batch(self.db, changed)
        # the rejected content never landed
        row = sqlite3.connect(self.db).execute(
            'SELECT start,end FROM spans').fetchone()
        self.assertEqual(row, (0, 1))


class RevisionAndRollback(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')
        apply_batch(self.db, batch('b1', [
            up(1, 't1', span('r', None, 0, 10, 'api')),
            up(2, 't2', span('r', None, 5, 15, 'api')),
        ]))
        apply_batch(self.db, batch('b2', [seal(3, 't1'), seal(4, 't2')]))

    def tearDown(self):
        self.td.cleanup()

    def test_linked_acceptance_histogram(self):
        result = query(self.db, {'service': 'api', 'start': 0, 'end': 20})
        self.assertEqual(result['histogram'], [
            {'concurrency': 0, 'ticks': 5},
            {'concurrency': 1, 'ticks': 10},
            {'concurrency': 2, 'ticks': 5},
        ])
        self.assertEqual(result['peak'], 2)
        self.assertEqual(result['occupied_ticks'], 15)
        self.assertEqual(result['trace_ticks'], 20)
        self.assertEqual(sum(b['ticks'] for b in result['histogram']), 20)

    def test_draft_revision_hides_old_contribution(self):
        apply_batch(self.db, batch('b3', [up(5, 't1', span('r', None, 0, 10, 'db'))]))
        api = query(self.db, {'service': 'api', 'start': 0, 'end': 20})
        dbq = query(self.db, {'service': 'db', 'start': 0, 'end': 20})
        self.assertEqual(api['peak'], 1)          # only t2 remains
        self.assertEqual(dbq['peak'], 0)          # draft contributes nothing
        apply_batch(self.db, batch('b4', [seal(6, 't1')]))
        self.assertEqual(query(self.db, {'service': 'db', 'start': 0, 'end': 20})['peak'], 1)
        self.assertEqual(query(self.db, {'service': 'api', 'start': 0, 'end': 20})['peak'], 1)

    def test_illegal_seal_after_revision_restores_everything(self):
        before = query(self.db, {'service': 'api', 'start': 0, 'end': 20})
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('b5', [
                up(7, 't1', span('r', None, 0, 10, 'db2')),
                seal(8, 'ghost'),
            ]))
        after = query(self.db, {'service': 'api', 'start': 0, 'end': 20})
        self.assertEqual(after, before)
        self.assertEqual(after['max_seq'], 4)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status,version FROM traces WHERE trace_id='t1'").fetchone(),
            ('sealed', 1))
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM bit_index WHERE service='db2'").fetchone()[0], 0)
        conn.close()

    def test_failed_reseal_keeps_old_contribution(self):
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('b5', [
                up(5, 't2', span('x', 'x', 0, 1, 'api')),
                seal(6, 't2'),
            ]))
        result = query(self.db, {'service': 'api', 'start': 0, 'end': 20})
        self.assertEqual(result['max_seq'], 4)
        self.assertEqual(result['histogram'], [
            {'concurrency': 0, 'ticks': 5},
            {'concurrency': 1, 'ticks': 10},
            {'concurrency': 2, 'ticks': 5}])

    def test_seal_then_upsert_within_one_batch_reopens_trace(self):
        # A seal followed by another upsert in the same batch must reopen the
        # trace to draft and revoke the just-written contributions atomically.
        apply_batch(self.db, batch('b5', [
            up(5, 't1', span('r', None, 0, 12, 'api')),
            seal(6, 't1'),
            up(7, 't1', span('r', None, 1, 11, 'db')),
        ]))
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status,version FROM traces WHERE trace_id='t1'").fetchone(),
            ('draft', 3))
        self.assertEqual(conn.execute(
            'SELECT count(*) FROM contrib_endpoints').fetchone()[0], 2)  # t2 only
        self.assertEqual(conn.execute(
            "SELECT count(*) FROM bit_index WHERE service='db'").fetchone()[0], 0)
        conn.close()
        self.assertEqual(query(self.db, {'service': 'db', 'start': 0, 'end': 12})['peak'], 0)
        # resealing the reopened trace enters analysis again
        apply_batch(self.db, batch('b6', [seal(8, 't1')]))
        self.assertEqual(query(self.db, {'service': 'db', 'start': 0, 'end': 12})['occupied_ticks'], 10)
        # double seal of a sealed trace is an error
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('b7', [seal(9, 't1'), seal(10, 't1')]))

    def test_failed_batch_rolls_back_earlier_events_in_same_batch(self):
        with self.assertRaises(ValueError):
            apply_batch(self.db, batch('b5', [
                up(5, 't1', span('r', None, 0, 10, 'db')),
                dele(6, 't1', 'nonexistent'),
            ]))
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT service FROM spans WHERE trace_id='t1'").fetchone(), ('api',))
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='max_seq'").fetchone()[0], '4')
        conn.close()


class GeometryAndQueries(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def test_child_written_before_parent_can_seal(self):
        apply_batch(self.db, batch('children', [
            up(1, 't', span('c', 'r', 2, 8, 'db')),   # parent missing: draft ok
        ]))
        apply_batch(self.db, batch('parent', [
            up(2, 't', span('r', None, 0, 10, 'api')),
            seal(3, 't'),
        ]))
        api = query(self.db, {'service': 'api', 'start': 0, 'end': 10})
        self.assertEqual(api['occupied_ticks'], 4)     # [0,2) + [8,10)
        dbq = query(self.db, {'service': 'db', 'start': 0, 'end': 10})
        self.assertEqual(dbq['occupied_ticks'], 6)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM traces WHERE trace_id='t'").fetchone()[0], 'sealed')
        conn.close()

    def test_parent_minus_direct_children_keeps_fragments(self):
        apply_batch(self.db, batch('b', [
            up(1, 't', span('r', None, 0, 10, 'api')),
            up(2, 't', span('a', 'r', 2, 8, 'db')),
            seal(3, 't'),
        ]))
        api = query(self.db, {'service': 'api', 'start': 0, 'end': 10})
        self.assertEqual(api['histogram'], [
            {'concurrency': 0, 'ticks': 6},
            {'concurrency': 1, 'ticks': 4}])
        dbq = query(self.db, {'service': 'db', 'start': 0, 'end': 10})
        self.assertEqual(dbq['occupied_ticks'], 6)

    def test_overlapping_same_service_siblings_count_once(self):
        # api root [0,20); two overlapping db children, one nested overlap
        apply_batch(self.db, batch('b', [
            up(1, 't', span('r', None, 0, 20, 'api')),
            up(2, 't', span('a', 'r', 2, 8, 'db')),
            up(3, 't', span('b', 'r', 6, 12, 'db')),
            up(4, 't', span('c', 'b', 7, 9, 'db')),
            seal(5, 't'),
        ]))
        dbq = query(self.db, {'service': 'db', 'start': 0, 'end': 20})
        self.assertEqual(dbq['peak'], 1)                 # union, not span count
        self.assertEqual(dbq['occupied_ticks'], 10)
        self.assertEqual(dbq['trace_ticks'], 10)
        api = query(self.db, {'service': 'api', 'start': 0, 'end': 20})
        self.assertEqual(api['occupied_ticks'], 10)     # [0,2) + [12,20)

    def test_touching_endpoints_and_zero_fragments(self):
        apply_batch(self.db, batch('b', [
            up(1, 't', span('r', None, 0, 10, 'svc')),
            up(2, 't', span('z', 'r', 3, 3, 'svc')),    # zero length
            up(3, 't', span('a', 'r', 3, 7, 'svc')),
            seal(4, 't'),
        ]))
        result = query(self.db, {'service': 'svc', 'start': 0, 'end': 10})
        self.assertEqual(result['histogram'], [{'concurrency': 1, 'ticks': 10}])
        self.assertEqual(result['peak'], 1)

    def test_multi_endpoint_same_tick_across_traces(self):
        apply_batch(self.db, batch('b1', [
            up(1, 't1', span('r', None, 5, 15, 'api')),
            up(2, 't2', span('r', None, 5, 15, 'api')),
            up(3, 't3', span('r', None, 5, 15, 'api')),
            seal(4, 't1'), seal(5, 't2'), seal(6, 't3'),
        ]))
        result = query(self.db, {'service': 'api', 'start': 0, 'end': 20})
        self.assertEqual(result['histogram'], [
            {'concurrency': 0, 'ticks': 10},
            {'concurrency': 3, 'ticks': 10}])

    def test_mid_window_base_count_is_not_assumed_zero(self):
        apply_batch(self.db, batch('b', [
            up(1, 't1', span('r', None, 0, 100, 'api')),
            up(2, 't2', span('r', None, 40, 60, 'api')),
            seal(3, 't1'), seal(4, 't2'),
        ]))
        result = query(self.db, {'service': 'api', 'start': 50, 'end': 70})
        self.assertEqual(result['histogram'], [
            {'concurrency': 1, 'ticks': 10},   # [60,70): t1 only
            {'concurrency': 2, 'ticks': 10}])  # [50,60): both
        self.assertEqual(sum(x['ticks'] for x in result['histogram']), 20)

    def test_unknown_service_returns_full_idle_window(self):
        apply_batch(self.db, batch('b', [
            up(1, 't', span('r', None, 0, 5, 'api')), seal(2, 't')]))
        result = query(self.db, {'service': 'other', 'start': 3, 'end': 9})
        self.assertEqual(result, {
            'service': 'other', 'start': 3, 'end': 9, 'max_seq': 2,
            'histogram': [{'concurrency': 0, 'ticks': 6}],
            'peak': 0, 'occupied_ticks': 0, 'trace_ticks': 0})

    def test_query_missing_db_file(self):
        missing = str(Path(self.td.name) / 'nope.db')
        result = query(missing, {'service': 'api', 'start': 0, 'end': 4})
        self.assertEqual(result['max_seq'], 0)
        self.assertEqual(result['histogram'], [{'concurrency': 0, 'ticks': 4}])
        self.assertFalse(Path(missing).exists())


class DurabilityAndConcurrency(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def test_reopen_across_processes(self):
        apply_batch(self.db, batch('b1', [
            up(1, 't', span('r', None, 0, 10, 'api')), seal(2, 't')]))
        again = apply_batch(self.db, batch('b2', [
            up(3, 't', span('a', 'r', 2, 8, 'db')), seal(4, 't')]))
        self.assertEqual(again['seq_from'], 3)
        # simulate a brand-new process by reconnecting with a fresh connection
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT status FROM traces WHERE trace_id='t'").fetchone()[0], 'sealed')
        conn.close()
        dbq = query(self.db, {'service': 'db', 'start': 0, 'end': 10})
        self.assertEqual(dbq['occupied_ticks'], 6)

    def test_commit_query_interleaving_never_shows_half_state(self):
        apply_batch(self.db, batch('seed', [
            up(1, 't1', span('r', None, 0, 10, 'api')), seal(2, 't1')]))
        barrier = threading.Barrier(2)
        seen = []

        def writer():
            barrier.wait()
            apply_batch(self.db, batch('grow', [
                up(3, 't2', span('r', None, 0, 10, 'api')), seal(4, 't2')]))

        def reader():
            barrier.wait()
            for _ in range(50):
                result = query(self.db, {'service': 'api', 'start': 0, 'end': 10})
                # snapshots must be internally consistent: either max_seq 2
                # with peak 1, or max_seq 4 with peak 2, never a mixture.
                if result['max_seq'] == 2:
                    seen.append((2, result['peak']))
                else:
                    self.assertEqual(result['max_seq'], 4)
                    seen.append((4, result['peak']))

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertTrue(any(seq == 4 for seq, _ in seen))
        for seq, peak in seen:
            self.assertEqual(peak, 1 if seq == 2 else 2)

    def test_kill_before_commit_then_retry_applies_once(self):
        apply_batch(self.db, batch('base', [
            up(1, 't', span('r', None, 0, 5, 'api')), seal(2, 't')]))
        pending = batch('pending', [
            up(3, 't2', span('r', None, 0, 7, 'api')), seal(4, 't2')])
        helper = Path(self.td.name) / 'crash_helper.py'
        helper.write_text(
            'import json, os, sys\n'
            'from trace_latency.warehouse import (connect, ensure_schema,\n'
            '    apply_locked, canonical_digest, validate_batch)\n'
            'data = json.load(open(sys.argv[2]))\n'
            '# Follow the production path: validate first, then one write txn.\n'
            'batch_id, events = validate_batch(data)\n'
            'conn = connect(sys.argv[1]); ensure_schema(conn)\n'
            'conn.execute("BEGIN IMMEDIATE")\n'
            'apply_locked(conn, batch_id, events, canonical_digest(data))\n'
            'os._exit(0)\n')
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        source = Path(self.td.name) / 'pending.json'
        source.write_text(json.dumps(pending))
        crashed = subprocess.run([sys.executable, str(helper), self.db,
                                  str(source)],
                                 env=env, capture_output=True, text=True)
        self.assertEqual(crashed.returncode, 0, crashed.stderr)
        # nothing from the killed transaction survived
        self.assertEqual(query(self.db, {'service': 'api', 'start': 0, 'end': 10})['max_seq'], 2)
        retry = apply_batch(self.db, pending)
        self.assertFalse(retry['replayed'])
        self.assertEqual((retry['seq_from'], retry['seq_to']), (3, 4))
        # exact retry afterwards is the idempotent replay
        again = apply_batch(self.db, pending)
        self.assertTrue(again['replayed'])
        self.assertEqual(again['seq_to'], 4)


class ScaleAndReadComplexity(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = str(Path(self.td.name) / 'w.db')

    def tearDown(self):
        self.td.cleanup()

    def test_small_window_does_not_scan_outside_history(self):
        near = 30
        far = 2000
        events = []
        seq = 1
        # near traces occupy [100,200) in service api (plain roots); a few
        # parent/child traces additionally exercise fragment geometry at scale.
        for i in range(near):
            tid = 'near%d' % i
            events += [
                up(seq, tid, span('r', None, 100, 200, 'api')),
                up(seq + 1, tid, span('a', 'r', 120, 160, 'db')),
                up(seq + 2, tid, span('b', 'r', 150, 190, 'db')),
                seal(seq + 3, tid),
            ]
            seq += 4
        for i in range(40):
            tid = 'plain%d' % i
            events += [
                up(seq, tid, span('r', None, 100, 200, 'api')),
                seal(seq + 1, tid),
            ]
            seq += 2
        # far traces live far outside the small query window
        for i in range(far):
            tid = 'far%d' % i
            events += [
                up(seq, tid, span('r', None, 9 * 10**11, 10**12, 'api')),
                seal(seq + 1, tid),
            ]
            seq += 2
        for i in range(0, len(events), 5000):
            chunk = events[i:i + 5000]
            apply_batch(self.db, batch('bulk%d' % i, chunk))
        conn = sqlite3.connect(self.db)
        total_traces = conn.execute('SELECT count(*) FROM traces').fetchone()[0]
        total_endpoints = conn.execute('SELECT count(*) FROM contrib_endpoints').fetchone()[0]
        total_spans = conn.execute('SELECT count(*) FROM spans').fetchone()[0]
        conn.close()
        self.assertEqual(total_traces, near + far + 40)
        self.assertGreater(total_endpoints, 4000)
        self.assertGreater(total_spans, 2000)

        statements = []
        import sqlite3 as _sqlite3
        original_sqlite_connect = _sqlite3.connect

        def instrumented_connect(path, *a, **kw):
            conn = original_sqlite_connect(path, *a, **kw)
            conn.set_trace_callback(lambda sql: statements.append(sql))
            return conn

        warehouse.sqlite3.connect = instrumented_connect
        try:
            small = query(self.db, {'service': 'api', 'start': 140, 'end': 160})
        finally:
            warehouse.sqlite3.connect = original_sqlite_connect
        joined = '\n'.join(statements)
        self.assertNotIn('spans', joined)              # never scan span storage
        self.assertNotIn('FROM traces', joined)
        # bounded work: left base <= 40 BIT hops + one windowed endpoint scan
        self.assertLess(len(statements), warehouse.LOG_U * 2 + 10)
        self.assertEqual(small['peak'], 40)
        self.assertEqual(small['occupied_ticks'], 20)

        # brute-force reference using only contribution endpoints
        conn = sqlite3.connect(self.db)
        rows = conn.execute(
            'SELECT pos,delta FROM contrib_endpoints WHERE service=? ORDER BY pos',
            ('api',)).fetchall()
        conn.close()
        start, end = 140, 160
        deltas = {}
        for pos, delta in rows:
            if start <= pos < end:
                deltas[pos] = deltas.get(pos, 0) + delta
        # base from full sweep up to start
        level = sum(delta for pos, delta in rows if pos <= start)
        cursor = start
        reference = {}
        for pos in sorted(deltas):
            reference[level] = reference.get(level, 0) + (pos - cursor)
            level += deltas[pos]
            cursor = pos
        reference[level] = reference.get(level, 0) + (end - cursor)
        expected = [{'concurrency': k, 'ticks': v}
                    for k, v in sorted(reference.items()) if v]
        self.assertEqual(small['histogram'], expected)
        self.assertEqual(sum(b['ticks'] for b in small['histogram']), end - start)
        self.assertEqual(small['trace_ticks'],
                         sum(b['concurrency'] * b['ticks'] for b in small['histogram']))


class Cli(unittest.TestCase):
    def call(self, *args):
        return subprocess.run([sys.executable, '-m', 'trace_latency.cli', *args],
                              capture_output=True, text=True, cwd=ROOT)

    def write_json(self, td, name, data):
        path = Path(td) / name
        path.write_text(json.dumps(data), encoding='utf-8')
        return path

    def test_warehouse_apply_and_query_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / 'w.db')
            source = self.write_json(td, 'batch.json', batch('b1', [
                up(1, 't1', span('r', None, 0, 10, 'api')),
                up(2, 't2', span('r', None, 5, 15, 'api')),
                seal(3, 't1'), seal(4, 't2'),
            ]))
            out = Path(td) / 'apply.json'
            r = self.call('warehouse-apply', '--db', db, '--source', str(source),
                          '--output', str(out))
            self.assertEqual(r.returncode, 0, r.stderr)
            report = json.loads(out.read_text())
            self.assertEqual(report['source_sha256'],
                             hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(report['result']['max_seq'], 4)
            qsource = self.write_json(td, 'q.json',
                                      {'service': 'api', 'start': 0, 'end': 20})
            qout = Path(td) / 'qout.json'
            r = self.call('warehouse-query', '--db', db,
                          '--source', str(qsource), '--output', str(qout))
            self.assertEqual(r.returncode, 0, r.stderr)
            qreport = json.loads(qout.read_text())
            self.assertEqual(qreport['source_sha256'],
                             hashlib.sha256(qsource.read_bytes()).hexdigest())
            self.assertEqual(qreport['result']['peak'], 2)

    def test_warehouse_failure_exit_2_preserves_old_output(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / 'w.db')
            out = Path(td) / 'out.json'
            out.write_text('prior')
            bad = self.write_json(td, 'bad.json', {'batch_id': 'b'})
            r = self.call('warehouse-apply', '--db', db, '--source', str(bad),
                          '--output', str(out))
            self.assertEqual(r.returncode, 2)
            self.assertEqual(out.read_text(), 'prior')
            qbad = self.write_json(td, 'qbad.json', {'service': 'api', 'start': 9, 'end': 1})
            r = self.call('warehouse-query', '--db', db,
                          '--source', str(qbad), '--output', str(out))
            self.assertEqual(r.returncode, 2)
            self.assertEqual(out.read_text(), 'prior')

    def test_8mb_and_duplicate_key_rules_apply(self):
        with tempfile.TemporaryDirectory() as td:
            db = str(Path(td) / 'w.db')
            big = Path(td) / 'big.json'
            big.write_text(json.dumps(batch('b', [
                up(1, 't', span('r' + 'x' * 100, None, 0, 1, 'api'))]))
                             + ' ' * (8 * 10**6 + 1))
            r = self.call('warehouse-apply', '--db', db, '--source', str(big))
            self.assertEqual(r.returncode, 2)
            dup = Path(td) / 'dup.json'
            dup.write_text('{"batch_id":"b","batch_id":"b","events":[]}')
            r = self.call('warehouse-apply', '--db', db, '--source', str(dup))
            self.assertEqual(r.returncode, 2)


if __name__ == '__main__':
    unittest.main()
