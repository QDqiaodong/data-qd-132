"""Tests for the offline tail-sampling collection pipeline (sample-traces)."""
import hashlib
import json
import os
import resource
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trace_latency.sampler import sample_traces

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'


def rec_span(tid, sid, parent, start, end, service, error=False):
    return {'kind': 'span', 'trace_id': tid,
            'span': {'id': sid, 'parent': parent, 'start': start, 'end': end,
                     'service': service, 'error': error}}


def rec_finish(tid, root_id, end, count):
    return {'kind': 'finish', 'trace_id': tid, 'root_id': root_id, 'end': end,
            'span_count': count}


def rec_wm(value):
    return {'kind': 'watermark', 'value': value}


def dump_line(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))


def digest_of(tid):
    return hashlib.sha256(trace_id_bytes(tid)).digest()


def trace_id_bytes(tid):
    return tid.encode('utf-8')


def ranked(trace_ids):
    """Digest-then-id byte order used for normal-budget selection."""
    return sorted(trace_ids,
                  key=lambda t: (digest_of(t), trace_id_bytes(t)))


class Case(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        base = Path(self.td.name)
        self.source = base / 'input.jsonl'
        self.state = base / 'state'
        self.output = base / 'out'
        self.policy = base / 'policy.json'
        self.write_policy(budget=1, slow=50)

    def tearDown(self):
        self.td.cleanup()

    # -- helpers ----------------------------------------------------------- #
    def write_policy(self, budget=1, slow=50, raw=None):
        if raw is not None:
            self.policy.write_text(raw, encoding='utf-8')
        else:
            self.policy.write_text(json.dumps(
                {'normal_budget': budget, 'slow_ticks': slow}),
                encoding='utf-8')

    def write_source(self, records, trailing_newline=True):
        lines = [r if isinstance(r, str) else dump_line(r) for r in records]
        text = '\n'.join(lines)
        if lines and trailing_newline:
            text += '\n'
        self.source.write_text(text, encoding='utf-8')

    def write_source_bytes(self, data):
        self.source.write_bytes(data)

    def run_pipeline(self):
        return sample_traces(self.source, self.state, self.output, self.policy)

    def fresh(self, records=None, policy=None):
        """Reset all directories, then optionally write policy and source."""
        self.setUp()
        if policy is not None:
            self.write_policy(raw=policy)
        if records is not None:
            self.write_source(records)

    def bad(self, records=None, policy=None):
        """Each failure case runs against fresh directories."""
        self.fresh(records, policy)
        with self.assertRaises((ValueError, OSError)):
            self.run_pipeline()

    def bad_bytes(self, data, policy=None):
        self.fresh(policy=policy)
        self.write_source_bytes(data)
        with self.assertRaises((ValueError, OSError)):
            self.run_pipeline()

    def cli(self, crash=None):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(SRC)
        if crash is not None:
            env['TRACE_LATENCY_SAMPLE_CRASH'] = crash
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', 'sample-traces',
             '--source', str(self.source), '--state', str(self.state),
             '--output', str(self.output), '--policy', str(self.policy)],
            capture_output=True, text=True, env=env)

    def read_jsonl(self, watermark, name):
        path = self.output / ('watermark-%d' % watermark) / name
        return [json.loads(line) for line in
                path.read_text(encoding='utf-8').splitlines()]

    def kept(self, watermark):
        return self.read_jsonl(watermark, 'kept.jsonl')

    def dropped(self, watermark):
        return self.read_jsonl(watermark, 'dropped.jsonl')

    def quarantine(self, watermark):
        return self.read_jsonl(watermark, 'quarantine.jsonl')

    def manifest(self, watermark):
        return json.loads((self.output / ('watermark-%d' % watermark)
                           / 'manifest.json').read_text(encoding='utf-8'))

    def tree_bytes(self):
        result = {}
        for path in sorted(self.output.rglob('*')):
            if path.is_file():
                result[str(path.relative_to(self.output))] = path.read_bytes()
        return result

    def db(self, sql, args=()):
        import sqlite3
        conn = sqlite3.connect(self.state / 'sampler.db')
        try:
            return conn.execute(sql, args).fetchall()
        finally:
            conn.close()


# --------------------------------------------------------------------------- #
class RecordContract(Case):
    def test_empty_line_rejected(self):
        self.bad_bytes(b'{"kind":"watermark","value":1}\n\n')

    def test_oversized_line_rejected(self):
        body = dump_line(rec_wm(1)) + ' ' * 1_000_000
        self.bad_bytes(body.encode() + b'\n')

    def test_exactly_1mb_line_accepted(self):
        self.fresh()
        record = dump_line(rec_wm(7))
        body = record + ' ' * (1_000_000 - len(record))
        self.assertEqual(len(body), 1_000_000)
        self.write_source_bytes(body.encode() + b'\n')
        report = self.run_pipeline()
        self.assertEqual(report['last_watermark'], 7)

    def test_invalid_utf8_rejected(self):
        self.bad_bytes(b'\xff\xfe\n')

    def test_duplicate_key_rejected(self):
        self.bad_bytes(b'{"kind":"watermark","kind":"watermark",'
                       b'"value":1}\n')

    def test_nonfinite_rejected(self):
        self.bad(['{"kind":"watermark","value":NaN}'])
        self.bad(['{"kind":"watermark","value":Infinity}'])
        self.bad(['{"kind":"watermark","value":1e999}'])

    def test_unknown_fields_and_kinds_rejected(self):
        self.bad([{'kind': 'watermark', 'value': 1, 'extra': 1}])
        self.bad([{'kind': 'span', 'trace_id': 't'}])
        self.bad([{'kind': 'span', 'trace_id': 't',
                   'span': {'id': 'r', 'parent': None, 'start': 0, 'end': 1,
                            'service': 'api', 'error': False, 'x': 1}}])
        self.bad([{'kind': 'finish', 'trace_id': 't', 'root_id': 'r',
                   'end': 1, 'span_count': 1, 'x': 1}])
        self.bad([{'kind': 'rewind', 'value': 1}])
        self.bad([{'value': 1}])
        self.bad(['[1,2]'])
        self.bad(['"text"'])

    def test_bool_rejected_for_ticks_counts_budget(self):
        self.bad([rec_span('t', 'r', None, True, 1, 'api')])
        self.bad([rec_span('t', 'r', None, 0, False, 'api')])
        self.bad([rec_finish('t', 'r', True, 1)])
        self.bad([{'kind': 'finish', 'trace_id': 't', 'root_id': 'r',
                   'end': 1, 'span_count': True}])
        self.bad([{'kind': 'watermark', 'value': False}])
        self.bad([rec_wm(1)], policy='{"normal_budget": true, "slow_ticks": 5}')
        self.bad([rec_wm(1)], policy='{"normal_budget": 1, "slow_ticks": false}')

    def test_ranges_and_span_shape(self):
        self.bad([rec_span('t', 'r', None, -1, 1, 'api')])
        self.bad([rec_span('t', 'r', None, 0, 10**12 + 1, 'api')])
        self.bad([rec_span('t', 'r', None, 5, 4, 'api')])
        self.bad([rec_span('t', ' r', None, 0, 1, 'api')])
        self.bad([rec_span('t', '', None, 0, 1, 'api')])
        self.bad([rec_span('t', 'r', None, 0, 1, '')])
        self.bad([rec_span('t', 'r', None, 0, 1, 'api', error='no')])
        self.bad([rec_span('t', 'r', None, 0, 1, 'api', error=1)])
        self.bad([rec_finish('t', 'r', 1, 0)])
        self.bad([rec_finish('t', 'r', 1, 5001)])
        self.bad([rec_finish('t', 'r', 1, 1.5)])
        self.bad([rec_wm(-1)])
        self.bad([rec_wm(10**12 + 1)])

    def test_policy_shape(self):
        self.bad([rec_wm(1)], policy='{"normal_budget": 1}')
        self.bad([rec_wm(1)],
                 policy='{"normal_budget": 1, "slow_ticks": 5, "x": 1}')
        self.bad([rec_wm(1)], policy='{"normal_budget": 10001, "slow_ticks": 5}')
        self.bad([rec_wm(1)],
                 policy='{"normal_budget": 1, "slow_ticks": %d}' % (10**12 + 1))
        self.bad([rec_wm(1)], policy='{"normal_budget": -1, "slow_ticks": 5}')
        # boundary values are accepted
        self.fresh([rec_wm(1)],
                   policy='{"normal_budget": 0, "slow_ticks": 0}')
        self.assertEqual(self.run_pipeline()['watermarks_published'], 1)
        self.fresh([rec_wm(1)],
                   policy='{"normal_budget": 10000, "slow_ticks": %d}' % 10**12)
        self.assertEqual(self.run_pipeline()['watermarks_published'], 1)

    def test_empty_source_is_valid(self):
        self.fresh()
        self.write_source_bytes(b'')
        report = self.run_pipeline()
        self.assertEqual(report['pending'], 0)
        self.assertEqual(report['watermarks_published'], 0)
        self.assertFalse(any(self.output.iterdir()))

    def test_last_line_without_newline(self):
        self.fresh()
        self.write_source([rec_span('t', 'r', None, 0, 1, 'api'),
                           rec_finish('t', 'r', 1, 1), rec_wm(5)],
                          trailing_newline=False)
        report = self.run_pipeline()
        self.assertEqual(report['watermarks_published'], 1)
        self.assertEqual(len(self.kept(5)), 1)

    def test_cli_exit_codes(self):
        self.fresh([rec_span('t', 'r', None, 0, 1, 'api'),
                    rec_finish('t', 'r', 1, 1), rec_wm(5)])
        ok = self.cli()
        self.assertEqual(ok.returncode, 0, ok.stderr)
        report = json.loads(ok.stdout)
        self.assertEqual(report['kind'], 'sample_traces')
        self.assertEqual(report['pending'], 0)
        self.fresh()
        self.write_source_bytes(b'not json\n')
        failed = self.cli()
        self.assertEqual(failed.returncode, 2)
        self.assertTrue(failed.stderr)


# --------------------------------------------------------------------------- #
class Assembly(Case):
    def test_finish_first_and_child_first_finalize(self):
        self.fresh([
            rec_finish('t1', 'r', 10, 2),                 # finish before spans
            rec_span('t1', 'c', 'r', 2, 8, 'db'),         # child before parent
            rec_span('t1', 'r', None, 0, 10, 'api'),
            rec_wm(10),
        ])
        report = self.run_pipeline()
        self.assertEqual(report['pending'], 0)
        kept = self.kept(10)
        self.assertEqual([k['trace_id'] for k in kept], ['t1'])
        self.assertEqual(kept[0]['reason'], 'normal')
        self.assertEqual(kept[0]['service'], 'api')
        self.assertEqual([s['id'] for s in kept[0]['spans']], ['c', 'r'])

    def test_cross_watermark_completion(self):
        self.fresh([
            rec_span('t1', 'a', 'r', 1, 2, 'db'),
            rec_wm(10),                                   # no finish yet: waits
            rec_span('t1', 'r', None, 0, 15, 'api'),
            rec_finish('t1', 'r', 15, 2),
            rec_wm(20),                                   # finalize here
        ])
        self.run_pipeline()
        self.assertEqual(self.kept(10), [])
        self.assertEqual([k['trace_id'] for k in self.kept(20)], ['t1'])

    def test_finish_end_above_watermark_waits(self):
        self.fresh([
            rec_span('t1', 'r', None, 0, 100, 'api'),
            rec_finish('t1', 'r', 100, 1),
            rec_wm(20),                                   # 100 > 20: waits
            rec_wm(100),
        ])
        self.run_pipeline()
        self.assertEqual(self.kept(20), [])
        self.assertEqual(len(self.kept(100)), 1)

    def test_quarantine_codes(self):
        cases = [
            ([rec_span('a', 'r', None, 0, 5, 'api')],
             rec_finish('a', 'r', 5, 2), 'span_count_mismatch'),
            ([rec_span('b', 'r', None, 0, 5, 'api'),
              rec_span('b', 's', None, 0, 5, 'api')],
             rec_finish('b', 'r', 5, 2), 'root_count'),
            ([rec_span('c', 'r', None, 0, 5, 'api'),
              rec_span('c', 'x', 'ghost', 1, 2, 'api')],
             rec_finish('c', 'r', 5, 2), 'unknown_parent'),
            ([rec_span('d', 'r', None, 0, 5, 'api'),
              rec_span('d', 'x', 'r', 4, 9, 'api')],
             rec_finish('d', 'r', 5, 2), 'interval_containment'),
            ([rec_span('e', 'r', None, 0, 5, 'api'),
              rec_span('e', 'x', 'y', 0, 1, 'api'),
              rec_span('e', 'y', 'x', 0, 1, 'api')],
             rec_finish('e', 'r', 5, 3), 'cycle'),
            ([rec_span('f', 'r', None, 0, 5, 'api')],
             rec_finish('f', 'other', 5, 1), 'root_mismatch'),
            ([rec_span('g', 'r', None, 0, 5, 'api')],
             rec_finish('g', 'r', 6, 1), 'root_end_mismatch'),
        ]
        records = []
        for spans, finish, _code in cases:
            records += spans + [finish]
        records.append(rec_wm(10))
        self.fresh(records)
        report = self.run_pipeline()
        self.assertEqual(report['quarantined'], len(cases))
        got = {q['trace_id']: q['reason'] for q in self.quarantine(10)}
        for spans, finish, code in cases:
            self.assertEqual(got[finish['trace_id']], code)
        self.assertEqual(self.kept(10), [])

    def test_quarantine_is_final_and_late_completion_fails(self):
        prefix = [
            rec_span('brk', 'r', None, 0, 5, 'api'),
            rec_span('brk', 'c', 'missing', 1, 2, 'api'),
            rec_finish('brk', 'r', 5, 2),
            rec_wm(10),
        ]
        # the missing parent arriving after the finalization watermark
        # cannot change the published quarantine: the run fails instead
        self.bad(prefix + [rec_span('brk', 'missing', 'r', 1, 2, 'api')])
        self.assertEqual(self.quarantine(10),
                         [{'trace_id': 'brk', 'reason': 'unknown_parent'}])
        # exact duplicates of the quarantined trace stay ignored
        self.fresh(prefix + [rec_span('brk', 'c', 'missing', 1, 2, 'api'),
                             rec_finish('brk', 'r', 5, 2)])
        report = self.run_pipeline()
        self.assertEqual(report['pending'], 0)
        self.assertEqual(self.quarantine(10),
                         [{'trace_id': 'brk', 'reason': 'unknown_parent'}])

    def test_conflicting_span_rejected_exact_duplicate_ignored(self):
        self.fresh([
            rec_span('t', 'r', None, 0, 5, 'api'),
            rec_span('t', 'r', None, 0, 5, 'api'),        # exact duplicate
            rec_finish('t', 'r', 5, 1),
            rec_finish('t', 'r', 5, 1),                   # exact duplicate
            rec_wm(10),
        ])
        report = self.run_pipeline()
        self.assertEqual(len(self.kept(10)), 1)
        self.assertEqual(report['pending'], 0)
        # key order does not affect duplicate equality
        self.fresh()
        self.write_source_bytes(
            b'{"kind":"span","trace_id":"t","span":{"id":"r","parent":null,'
            b'"start":0,"end":5,"service":"api","error":false}}\n'
            b'{"span":{"error":false,"service":"api","end":5,"start":0,'
            b'"parent":null,"id":"r"},"trace_id":"t","kind":"span"}\n'
            b'{"kind":"finish","trace_id":"t","root_id":"r","end":5,'
            b'"span_count":1}\n{"kind":"watermark","value":10}\n')
        self.assertEqual(self.run_pipeline()['pending'], 0)
        # conflicting span content
        self.bad([rec_span('t', 'r', None, 0, 5, 'api'),
                  rec_span('t', 'r', None, 0, 6, 'api')])
        # conflicting finish content
        self.bad([rec_finish('t', 'r', 5, 1), rec_finish('t', 'r', 6, 1)])

    def test_post_finalization_records(self):
        prefix = [rec_span('t', 'r', None, 0, 5, 'api'),
                  rec_finish('t', 'r', 5, 1), rec_wm(10)]
        # exact duplicates after finalization are ignored
        self.fresh(prefix + [rec_span('t', 'r', None, 0, 5, 'api'),
                             rec_finish('t', 'r', 5, 1)])
        self.assertEqual(self.run_pipeline()['pending'], 0)
        # new span id after finalization
        self.bad(prefix + [rec_span('t', 'other', 'r', 1, 2, 'api')])
        # conflicting span after finalization
        self.bad(prefix + [rec_span('t', 'r', None, 0, 9, 'api')])
        # conflicting finish after finalization
        self.bad(prefix + [rec_finish('t', 'r', 6, 1)])

    def test_retroactive_finish_rejected(self):
        self.bad([rec_wm(10), rec_finish('late', 'r', 5, 1)])
        self.bad([rec_wm(10), rec_finish('late', 'r', 10, 1)])  # == not greater
        self.fresh([rec_wm(10),
                    rec_span('late', 'r', None, 0, 15, 'api'),
                    rec_finish('late', 'r', 15, 1), rec_wm(20)])
        self.run_pipeline()
        self.assertEqual(len(self.kept(20)), 1)

    def test_watermark_must_strictly_increase(self):
        self.bad([rec_wm(10), rec_wm(10)])
        self.bad([rec_wm(10), rec_wm(9)])

    def test_trace_span_limit(self):
        records = [rec_span('big', 'r', None, 0, 5000, 'api')]
        records += [rec_span('big', 's%04d' % i, 'r', 0, 1, 'api')
                    for i in range(1, 5000)]
        records += [rec_finish('big', 'r', 5000, 5000), rec_wm(5000)]
        self.fresh(records)
        self.run_pipeline()
        kept = self.kept(5000)
        self.assertEqual(len(kept), 1)
        self.assertEqual(len(kept[0]['spans']), 5000)
        # the 5001st distinct span of one trace is rejected
        self.bad([rec_span('big', 's%04d' % i, 'r', 0, 1, 'api')
                  for i in range(5001)])


# --------------------------------------------------------------------------- #
class Sampling(Case):
    def normal_trace(self, tid, service='api', start=0, end=10):
        return [rec_span(tid, 'r', None, start, end, service),
                rec_finish(tid, 'r', end, 1)]

    def test_digest_order_selects_fixed_winner(self):
        ids = ranked(['alpha', 'beta', 'gamma'])
        records = []
        for tid in ids:                                   # input order irrelevant
            records += self.normal_trace(tid)
        records.append(rec_wm(10))
        self.fresh(records)
        self.run_pipeline()
        kept = self.kept(10)
        self.assertEqual([k['trace_id'] for k in kept], [ids[0]])
        self.assertEqual(kept[0]['reason'], 'normal')
        self.assertEqual([d['trace_id'] for d in self.dropped(10)],
                         sorted(ids[1:], key=trace_id_bytes))
        self.assertTrue(all(d['reason'] == 'budget'
                            for d in self.dropped(10)))

    def test_input_order_does_not_change_selection(self):
        ids = ranked(['n%d' % i for i in range(6)])
        records = []
        for tid in ids:
            records += self.normal_trace(tid)
        self.fresh(records + [rec_wm(10)])
        self.run_pipeline()
        # reversed input order, fresh state: identical decisions (manifests
        # differ only because the source bytes themselves differ)
        first = {name: (self.output / 'watermark-10' / name).read_bytes()
                 for name in ('kept.jsonl', 'dropped.jsonl',
                              'quarantine.jsonl')}
        self.fresh(list(reversed(records)) + [rec_wm(10)])
        self.run_pipeline()
        second = {name: (self.output / 'watermark-10' / name).read_bytes()
                  for name in ('kept.jsonl', 'dropped.jsonl',
                               'quarantine.jsonl')}
        self.assertEqual(first, second)

    def test_budgets_are_per_service(self):
        records = []
        for tid in ['a1', 'a2']:
            records += self.normal_trace(tid, service='api')
        for tid in ['d1', 'd2']:
            records += self.normal_trace(tid, service='db')
        records.append(rec_wm(10))
        self.fresh(records)
        self.run_pipeline()
        kept = self.kept(10)
        self.assertEqual(len(kept), 2)                    # one per service
        self.assertEqual(sorted(k['service'] for k in kept), ['api', 'db'])
        self.assertEqual(len(self.dropped(10)), 2)

    def test_zero_budget_keeps_only_error_and_slow(self):
        records = self.normal_trace('plain')
        records += [rec_span('err', 'r', None, 0, 5, 'api', error=True),
                    rec_finish('err', 'r', 5, 1)]
        records += [rec_span('slow', 'r', None, 0, 60, 'api'),
                    rec_finish('slow', 'r', 60, 1), rec_wm(60)]
        self.fresh(records, policy='{"normal_budget": 0, "slow_ticks": 50}')
        self.run_pipeline()
        kept = {k['trace_id']: k['reason'] for k in self.kept(60)}
        self.assertEqual(kept, {'err': 'error', 'slow': 'slow'})
        self.assertEqual([d['trace_id'] for d in self.dropped(60)], ['plain'])

    def test_reason_priority_and_slow_boundary(self):
        records = [
            rec_span('both', 'r', None, 0, 80, 'api', error=True),
            rec_finish('both', 'r', 80, 1),               # error wins over slow
            rec_span('edge', 'r', None, 0, 50, 'api'),
            rec_finish('edge', 'r', 50, 1),               # duration == slow_ticks
            rec_span('under', 'r', None, 0, 49, 'api'),
            rec_finish('under', 'r', 49, 1),              # just below: normal
            rec_wm(80),
        ]
        self.fresh(records)
        self.run_pipeline()
        kept = {k['trace_id']: k['reason'] for k in self.kept(80)}
        self.assertEqual(kept['both'], 'error')
        self.assertEqual(kept['edge'], 'slow')
        self.assertEqual(kept['under'], 'normal')         # budget slot

    def test_kept_spans_sorted_with_all_fields(self):
        self.fresh([
            rec_span('t', 'b', 'r', 1, 2, 'db', error=True),
            rec_span('t', 'a', 'r', 0, 1, 'api'),
            rec_span('t', 'r', None, 0, 3, 'api'),
            rec_finish('t', 'r', 3, 3),
            rec_wm(5),
        ])
        self.run_pipeline()
        kept = self.kept(5)
        self.assertEqual([s['id'] for s in kept[0]['spans']], ['a', 'b', 'r'])
        self.assertEqual(set(kept[0]['spans'][0]),
                         {'id', 'parent', 'start', 'end', 'service', 'error'})
        self.assertEqual(kept[0]['reason'], 'error')      # b has error=true

    def test_outputs_sorted_by_trace_id_utf8_bytes(self):
        ids = ['z', 'a', 'é', '中']
        records = []
        for tid in ids:                                   # deliberately unsorted
            records += [rec_span(tid, 'r', None, 0, 5, 'api', error=True),
                        rec_finish(tid, 'r', 5, 1)]
        records.append(rec_wm(5))
        self.fresh(records)
        self.run_pipeline()
        order = sorted(ids, key=trace_id_bytes)
        self.assertEqual([k['trace_id'] for k in self.kept(5)], order)

    def test_manifest_contents(self):
        self.fresh([rec_span('t', 'r', None, 0, 5, 'api'),
                    rec_finish('t', 'r', 5, 1), rec_wm(5)])
        self.run_pipeline()
        manifest = self.manifest(5)
        self.assertEqual(manifest['watermark'], 5)
        self.assertEqual(manifest['source_sha256'],
                         hashlib.sha256(self.source.read_bytes()).hexdigest())
        self.assertEqual(manifest['policy_sha256'],
                         hashlib.sha256(self.policy.read_bytes()).hexdigest())
        files = manifest['files']
        self.assertEqual(set(files),
                         {'kept.jsonl', 'dropped.jsonl', 'quarantine.jsonl'})
        for name, info in files.items():
            raw = (self.output / 'watermark-5' / name).read_bytes()
            self.assertEqual(info['sha256'], hashlib.sha256(raw).hexdigest())
            self.assertEqual(info['count'],
                             0 if not raw else len(raw.splitlines()))
        self.assertEqual(files['kept.jsonl']['count'], 1)

    def test_empty_watermark_still_publishes(self):
        self.fresh([rec_wm(3), rec_wm(7)])
        report = self.run_pipeline()
        self.assertEqual(report['watermarks_published'], 2)
        for value in (3, 7):
            manifest = self.manifest(value)
            for name, info in manifest['files'].items():
                self.assertEqual(info['count'], 0)
                self.assertEqual(info['sha256'], hashlib.sha256(b'').hexdigest())
                self.assertEqual((self.output / ('watermark-%d' % value) /
                                  name).read_bytes(), b'')


# --------------------------------------------------------------------------- #
class LinkedAcceptance(Case):
    def test_quarantined_favourite_frees_the_budget_slot(self):
        small, large = ranked(['cand-%03d' % i for i in range(40)])[:2]
        records = [
            # smaller digest candidate cannot seal: parent reference missing
            rec_span(small, 'r', None, 0, 10, 'api'),
            rec_span(small, 'c', 'ghost', 1, 2, 'api'),
            rec_finish(small, 'r', 10, 2),
            # larger digest candidate is complete and normal
            rec_span(large, 'r', None, 0, 10, 'api'),
            rec_finish(large, 'r', 10, 1),
            rec_wm(10),
        ]
        self.fresh(records)
        self.run_pipeline()
        self.assertEqual(self.quarantine(10),
                         [{'trace_id': small, 'reason': 'unknown_parent'}])
        kept = self.kept(10)
        self.assertEqual([k['trace_id'] for k in kept], [large])
        self.assertEqual(kept[0]['reason'], 'normal')     # got the only slot
        self.assertEqual(self.dropped(10), [])

    def test_late_error_trace_kept_only_at_its_own_watermark(self):
        records = [
            rec_span('early', 'r', None, 0, 5, 'api'),
            rec_finish('early', 'r', 5, 1),
            rec_span('late', 'r', None, 0, 15, 'api', error=True),
            rec_wm(10),                                   # late has no finish
            rec_finish('late', 'r', 15, 1),
            rec_wm(20),
        ]
        self.fresh(records)
        self.run_pipeline()
        self.assertEqual([k['trace_id'] for k in self.kept(10)], ['early'])
        self.assertEqual(self.quarantine(10), [])
        kept = self.kept(20)
        self.assertEqual([k['trace_id'] for k in kept], ['late'])
        self.assertEqual(kept[0]['reason'], 'error')


# --------------------------------------------------------------------------- #
class Recovery(Case):
    def rich_source(self):
        ids = ranked(['n%d' % i for i in range(4)])
        records = [
            rec_span('e1', 'r', None, 0, 5, 'api', error=True),
            rec_finish('e1', 'r', 5, 1),
            rec_span('q1', 'r', None, 0, 5, 'api'),
            rec_finish('q1', 'r', 5, 3),                  # count mismatch
            rec_wm(10),
        ]
        for tid in ids[:2]:                               # one kept, one dropped
            records += [rec_span(tid, 'r', None, 0, 15, 'api'),
                        rec_finish(tid, 'r', 15, 1)]
        records += [rec_span('s2', 'r', None, 0, 90, 'api'),
                    rec_finish('s2', 'r', 90, 1),
                    rec_wm(20),
                    rec_wm(30)]                           # empty watermark
        return records

    def test_cross_process_reopen_is_noop(self):
        self.fresh(self.rich_source())
        first = self.cli()
        self.assertEqual(first.returncode, 0, first.stderr)
        tree = self.tree_bytes()
        again = self.cli()
        self.assertEqual(again.returncode, 0, again.stderr)
        report = json.loads(again.stdout)
        self.assertEqual(report['watermarks_published'], 0)
        self.assertEqual(report['last_watermark'], 30)
        self.assertEqual(self.tree_bytes(), tree)

    def test_crash_at_all_three_boundaries(self):
        self.fresh(self.rich_source())
        reference = self.cli()
        self.assertEqual(reference.returncode, 0, reference.stderr)
        expected = self.tree_bytes()
        expected_report = json.loads(reference.stdout)
        for seq in (1, 2, 3):
            for stage in ('files', 'rename', 'commit'):
                self.fresh(self.rich_source())
                crashed = self.cli(crash='%s:%d' % (stage, seq))
                self.assertNotEqual(crashed.returncode, 0)
                value = (10, 20, 30)[seq - 1]
                staging = self.output / ('.staging-%d' % value)
                visible = self.output / ('watermark-%d' % value)
                if stage == 'files':
                    self.assertTrue(staging.is_dir())
                    self.assertFalse(visible.exists())
                else:
                    self.assertFalse(staging.exists())
                    self.assertTrue(visible.is_dir())
                inode = visible.stat().st_ino if visible.exists() else None
                rerun = self.cli()
                self.assertEqual(rerun.returncode, 0, rerun.stderr)
                # byte-identical dataset to the one-shot run
                self.assertEqual(self.tree_bytes(), expected)
                report = json.loads(rerun.stdout)
                self.assertEqual(report['last_watermark'],
                                 expected_report['last_watermark'])
                self.assertEqual(report['pending'], expected_report['pending'])
                if inode is not None:
                    # a published watermark directory is never replaced
                    self.assertEqual(visible.stat().st_ino, inode)

    def test_identity_change_rejected_and_dataset_kept(self):
        self.fresh(self.rich_source())
        self.assertEqual(self.cli().returncode, 0)
        tree = self.tree_bytes()
        self.write_source(self.rich_source() + [rec_wm(40)])
        self.assertEqual(self.cli().returncode, 2)        # source changed
        self.assertEqual(self.tree_bytes(), tree)
        self.fresh(self.rich_source())
        self.assertEqual(self.cli().returncode, 0)
        tree = self.tree_bytes()
        self.write_policy(budget=2, slow=50)              # policy changed
        self.assertEqual(self.cli().returncode, 2)
        self.assertEqual(self.tree_bytes(), tree)

    def test_input_error_keeps_only_published_watermarks(self):
        self.fresh()
        self.write_source_bytes(
            (dump_line(rec_span('t', 'r', None, 0, 5, 'api')) + '\n'
             + dump_line(rec_finish('t', 'r', 5, 1)) + '\n'
             + dump_line(rec_wm(10)) + '\n'
             + '{"kind":"span",broken\n'
             + dump_line(rec_wm(20)) + '\n').encode('utf-8'))
        with self.assertRaises((ValueError, OSError)):
            self.run_pipeline()
        self.assertTrue((self.output / 'watermark-10').is_dir())
        self.assertFalse((self.output / 'watermark-20').exists())
        self.assertEqual([p for p in self.output.iterdir()
                          if p.name.startswith('.staging-')], [])
        self.assertEqual(len(self.kept(10)), 1)

    def test_eof_pending_persists_without_double_consume(self):
        self.fresh([rec_span('t', 'r', None, 0, 5, 'api'),
                    rec_finish('t', 'r', 5, 1)])
        report = self.run_pipeline()
        self.assertEqual(report['pending'], 1)
        self.assertEqual(report['watermarks_published'], 0)
        self.assertFalse(any(self.output.iterdir()))
        self.assertEqual(self.db('SELECT COUNT(*) FROM spans'), [(1,)])
        again = self.run_pipeline()
        self.assertEqual(again['pending'], 1)
        self.assertEqual(self.db('SELECT COUNT(*) FROM spans'), [(1,)])

    def test_foreign_published_directory_is_never_replaced(self):
        self.fresh([rec_span('t', 'r', None, 0, 5, 'api'),
                    rec_finish('t', 'r', 5, 1), rec_wm(10)])
        foreign = self.output / 'watermark-10'
        foreign.mkdir(parents=True)
        (foreign / 'kept.jsonl').write_text('foreign\n', encoding='utf-8')
        with self.assertRaises((ValueError, OSError)):
            self.run_pipeline()
        self.assertEqual((foreign / 'kept.jsonl').read_bytes(), b'foreign\n')


# --------------------------------------------------------------------------- #
class Scale(Case):
    def rss_kib(self):
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    def warm_up(self):
        self.fresh([rec_span('warm', 'r', None, 0, 1, 'api'), rec_wm(5)])
        self.run_pipeline()

    def test_million_waiting_traces_bounded_memory(self):
        self.warm_up()
        self.fresh()
        total = 1_000_000
        with self.source.open('w', encoding='utf-8') as stream:
            for i in range(total):
                # stride order: consecutive lines belong to distant traces
                tid = 't%08d' % ((i * 7919) % total)
                stream.write(dump_line(
                    rec_span(tid, 'r', None, 0, 1, 'api')) + '\n')
        before = self.rss_kib()
        report = self.run_pipeline()
        used = self.rss_kib() - before
        self.assertEqual(report['pending'], total)
        self.assertLess(used, 64 * 1024, 'working memory grew past 64 MiB')
        self.assertEqual(self.db("SELECT COUNT(*) FROM traces "
                                 "WHERE status='waiting'"), [(total,)])

    def test_large_candidate_set_bounded_memory(self):
        self.warm_up()
        self.fresh(policy='{"normal_budget": 3, "slow_ticks": 50}')
        total = 50_000
        ids = ['c%08d' % i for i in range(total)]
        with self.source.open('w', encoding='utf-8') as stream:
            for tid in ids:
                stream.write(dump_line(rec_span(tid, 'r', None, 0, 1, 'api'))
                             + '\n')
                stream.write(dump_line(rec_finish(tid, 'r', 1, 1)) + '\n')
            stream.write(dump_line(rec_wm(10)) + '\n')
        before = self.rss_kib()
        report = self.run_pipeline()
        used = self.rss_kib() - before
        self.assertLess(used, 64 * 1024)
        self.assertEqual(report['kept'], 3)
        self.assertEqual(report['dropped'], total - 3)
        winners = ranked(ids)[:3]
        self.assertEqual([k['trace_id'] for k in self.kept(10)],
                         sorted(winners, key=trace_id_bytes))
        dropped_ids = [d['trace_id'] for d in self.dropped(10)]
        self.assertEqual(dropped_ids, sorted(dropped_ids, key=trace_id_bytes))
        self.assertEqual(len(dropped_ids), total - 3)

    def test_many_watermarks(self):
        records = []
        for w in range(1, 21):
            tid = 't%02d' % w
            # root end must exceed the previously published watermark
            records += [rec_span(tid, 'r', None, 10 * (w - 1), 10 * w, 'api'),
                        rec_finish(tid, 'r', 10 * w, 1), rec_wm(w * 10)]
        self.fresh(records)
        report = self.run_pipeline()
        self.assertEqual(report['watermarks_published'], 20)
        self.assertEqual(report['pending'], 0)
        for w in range(1, 21):
            self.assertEqual([k['trace_id'] for k in self.kept(w * 10)],
                             ['t%02d' % w])


if __name__ == '__main__':
    unittest.main()
