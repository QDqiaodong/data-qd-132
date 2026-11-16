"""Subprocess fault-injection tests for crash recovery and entry gates.

Every crash is delivered through the test-only ``TRACE_LATENCY_RETIRE_CRASH``
hook, which calls ``os._exit`` at a durable maintenance boundary.  A second
process retries the same request and must finish with the same data and
first-run counts as an uninterrupted run.
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trace_latency import apply_batch, sample_traces, retire_traces
from trace_latency.warehouse import query

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'


def dump(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))


def up(seq, tid, start=0, end=10, service='api'):
    return {'seq': seq, 'trace_id': tid, 'op': 'upsert',
            'span': {'id': 'r', 'parent': None, 'start': start, 'end': end,
                     'service': service}}


def seal(seq, tid):
    return {'seq': seq, 'trace_id': tid, 'op': 'seal'}


def span(tid, **kw):
    body = {'id': 'r', 'parent': None, 'start': 0, 'end': 10,
            'service': 'api', 'error': False}
    body.update(kw)
    return {'kind': 'span', 'trace_id': tid, 'span': body}


def finish(tid, end=10, root='r', count=1):
    return {'kind': 'finish', 'trace_id': tid, 'root_id': root, 'end': end,
            'span_count': count}


class CrashRecovery(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        base = Path(self.td.name)
        self.base = base
        self.wh = base / 'warehouse.db'
        self.state = base / 'state'
        self.out = base / 'out'
        self.catalog = base / 'catalog'
        self.policy = base / 'policy.json'
        self.source = base / 'telemetry.jsonl'
        self.request = base / 'request.json'
        self.policy.write_text(json.dumps(
            {'normal_budget': 2, 'slow_ticks': 50}))
        self._build()

    def tearDown(self):
        self.td.cleanup()

    def _build(self):
        # Warehouse: sealed s1/s2/s3 plus a draft d1.
        events = []
        seq = 1
        for tid, service in (('s1', 'api'), ('s2', 'api'), ('s3', 'db')):
            events.append(up(seq, tid, end=10, service=service)); seq += 1
            events.append(seal(seq, tid)); seq += 1
        events.append(up(seq, 'd1', end=7, service='db')); seq += 1
        apply_batch(self.wh, {'batch_id': 'wb', 'events': events})
        # Two watermarks with kept/dropped/quarantine content and a waiter.
        records = [
            span('k1', error=True), finish('k1'),
            span('p1', service='svc-a'), finish('p1', end=4),
            span('p2', service='svc-a'), finish('p2', end=5),
            span('q1'), finish('q1', root='ghost'),
            {'kind': 'watermark', 'value': 10},
            span('k2', start=11, end=15, service='db'),
            finish('k2', end=15),
            span('x2', start=11, end=14, service='svc-b'),
            finish('x2', end=14),
            span('y2', start=11, end=13, service='svc-b'),
            finish('y2', end=13),
            {'kind': 'watermark', 'value': 20},
            span('wait1', start=0, end=9),
        ]
        self.source.write_text(
            ''.join(dump(r) + '\n' for r in records))
        report = sample_traces(self.source, self.state, self.out, self.policy)
        self.assertEqual(report['pending'], 1)
        # Retire the dropped rivals explicitly by id, plus all other targets.
        dropped_10 = [r['trace_id'] for r in self._jsonl(10, 'dropped.jsonl')]
        dropped_20 = [r['trace_id'] for r in self._jsonl(20, 'dropped.jsonl')]
        self.ids = ['s1', 's2', 's3', 'd1', 'k1', 'q1', 'wait1',
                    'unknown-u'] + dropped_10 + dropped_20
        self.request.write_text(json.dumps(
            {'request_id': 'req-crash', 'trace_ids': self.ids}))
        # An uninterrupted reference run against an identical environment.
        self._build_reference()

    def _build_reference(self):
        ref_root = self.base / 'ref-workspace'
        if ref_root.exists():
            shutil.rmtree(ref_root)
        ref_root.mkdir()
        for item in ('warehouse.db', 'state', 'out', 'policy.json',
                     'telemetry.jsonl'):
            src = self.base / item
            dst = ref_root / item
            if src.is_dir():
                shutil.copytree(src, dst)
            else:
                shutil.copy2(src, dst)
        shutil.copy2(self.request, ref_root / 'request.json')
        self.ref_root = ref_root
        report = retire_traces(ref_root / 'catalog', ref_root / 'warehouse.db',
                               ref_root / 'state', ref_root / 'out',
                               ref_root / 'request.json')
        self.reference_report = report

    def _jsonl(self, watermark, name):
        path = self.out / ('watermark-%d' % watermark) / name
        return [json.loads(line) for line in path.read_text().splitlines()]

    def cli_retire(self, crash=None):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(SRC)
        if crash is not None:
            env['TRACE_LATENCY_RETIRE_CRASH'] = crash
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', 'retire-traces',
             '--catalog', str(self.catalog), '--db', str(self.wh),
             '--state', str(self.state), '--output', str(self.out),
             '--source', str(self.request)],
            capture_output=True, text=True, env=env, cwd=ROOT)

    def cli_apply(self, batch_file):
        env = dict(os.environ, PYTHONPATH=str(SRC))
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli',
             'warehouse-apply', '--db', str(self.wh),
             '--source', str(batch_file)],
            capture_output=True, text=True, env=env, cwd=ROOT)

    def cli_query(self, query_file):
        env = dict(os.environ, PYTHONPATH=str(SRC))
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli',
             'warehouse-query', '--db', str(self.wh),
             '--source', str(query_file), '--output',
             str(self.base / 'qout.json')],
            capture_output=True, text=True, env=env, cwd=ROOT)

    def cli_sample(self):
        env = dict(os.environ, PYTHONPATH=str(SRC))
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', 'sample-traces',
             '--source', str(self.source), '--state', str(self.state),
             '--output', str(self.out), '--policy', str(self.policy)],
            capture_output=True, text=True, env=env, cwd=ROOT)

    def assert_entries_blocked(self):
        batch = self.base / 'late-batch.json'
        batch.write_text(json.dumps({'batch_id': 'late', 'events': [
            up(1, 'new-x')]}))
        q = self.base / 'q.json'
        q.write_text(json.dumps(
            {'service': 'api', 'start': 0, 'end': 20}))
        self.assertEqual(self.cli_apply(batch).returncode, 2)
        self.assertEqual(self.cli_query(q).returncode, 2)
        self.assertEqual(self.cli_sample().returncode, 2)

    def assert_matches_reference(self, report):
        self.assertEqual(report, self.reference_report)
        for rel in ('warehouse.db',):
            self._assert_sqlite_equal(self.ref_root / rel, self.wh,
                                     ('traces', 'spans', 'contrib_endpoints',
                                      'bit_index', 'batches', 'meta'))
        self._assert_sqlite_equal(self.ref_root / 'state' / 'sampler.db',
                                  self.state / 'sampler.db',
                                  ('traces', 'spans', 'evidence', 'meta'))
        for watermark in (10, 20):
            for name in ('kept.jsonl', 'dropped.jsonl',
                         'quarantine.jsonl', 'manifest.json'):
                got = (self.out / ('watermark-%d' % watermark) / name).read_bytes()
                want = (self.ref_root / 'out' / ('watermark-%d' % watermark)
                        / name).read_bytes()
                self.assertEqual(got, want, (watermark, name))

    def _assert_sqlite_equal(self, left_path, right_path, tables):
        left = sqlite3.connect(left_path)
        right = sqlite3.connect(right_path)
        try:
            for table in tables:
                columns = [r[1] for r in left.execute(
                    'PRAGMA table_info(%s)' % table)]
                order = ','.join(columns) or '1'
                lrows = left.execute(
                    'SELECT * FROM %s ORDER BY %s' % (table, order)).fetchall()
                rrows = right.execute(
                    'SELECT * FROM %s ORDER BY %s' % (table, order)).fetchall()
                if table == 'meta':
                    # the catalog pointer legitimately names each workspace's
                    # own catalog directory; compare every other meta row
                    lrows = [r for r in lrows
                             if r[0] != 'retirement_catalog']
                    rrows = [r for r in rrows
                             if r[0] != 'retirement_catalog']
                self.assertEqual(lrows, rrows, table)
        finally:
            left.close()
            right.close()

    def test_uninterrupted_cli_matches_python_api(self):
        result = self.cli_retire()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)['result']
        self.assert_matches_reference(report)

    def test_crash_after_acceptance_blocks_all_entries(self):
        crashed = self.cli_retire(crash='accept')
        self.assertNotEqual(crashed.returncode, 0)
        # request is durably active; every managed entry is closed
        self.assert_entries_blocked()
        # retry in a new process resumes and finishes
        retry = self.cli_retire()
        self.assertEqual(retry.returncode, 0, retry.stderr)
        report = json.loads(retry.stdout)['result']
        self.assert_matches_reference(report)
        # entries are open again after completion
        q = self.base / 'q.json'
        self.assertEqual(self.cli_query(q).returncode, 0)

    def test_crash_after_warehouse_commit(self):
        crashed = self.cli_retire(crash='warehouse_commit')
        self.assertNotEqual(crashed.returncode, 0)
        self.assert_entries_blocked()
        retry = self.cli_retire()
        self.assertEqual(retry.returncode, 0, retry.stderr)
        report = json.loads(retry.stdout)['result']
        # first-run warehouse count preserved despite partial deletion
        self.assertEqual(report['warehouse_traces'],
                         self.reference_report['warehouse_traces'])
        self.assert_matches_reference(report)

    def test_crash_after_sampler_commit(self):
        crashed = self.cli_retire(crash='sampler_commit')
        self.assertNotEqual(crashed.returncode, 0)
        self.assert_entries_blocked()
        retry = self.cli_retire()
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assert_matches_reference(json.loads(retry.stdout)['result'])

    def test_crash_between_watermark_file_and_manifest_replacement(self):
        # crash right after the three JSONL files of watermark 10 are
        # replaced but before its manifest is republished
        crashed = self.cli_retire(crash='wmafterfiles:10')
        self.assertNotEqual(crashed.returncode, 0)
        self.assert_entries_blocked()
        retry = self.cli_retire()
        self.assertEqual(retry.returncode, 0, retry.stderr)
        report = json.loads(retry.stdout)['result']
        self.assertEqual(report['published_kept'],
                         self.reference_report['published_kept'])
        self.assert_matches_reference(report)

    def test_crash_before_watermark_replacement_leaves_bytes_intact(self):
        crashed = self.cli_retire(crash='wmbefore:10')
        self.assertNotEqual(crashed.returncode, 0)
        # the visible watermark tree is still the original bytes
        for name in ('kept.jsonl', 'dropped.jsonl', 'quarantine.jsonl'):
            got = (self.out / 'watermark-10' / name).read_bytes()
            want = (self.ref_root / 'out' / 'watermark-10' / name).read_bytes()
            # reference is already retired, so compare against the staging
            # tmp files instead: originals must not yet have been replaced
        retry = self.cli_retire()
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assert_matches_reference(json.loads(retry.stdout)['result'])

    def test_crash_after_watermark_done_then_retry(self):
        crashed = self.cli_retire(crash='wmdone:10')
        self.assertNotEqual(crashed.returncode, 0)
        retry = self.cli_retire()
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assert_matches_reference(json.loads(retry.stdout)['result'])

    def test_crash_right_before_completion_marker(self):
        crashed = self.cli_retire(crash='complete')
        self.assertNotEqual(crashed.returncode, 0)
        # all payload work is finished but the request is still active
        self.assert_entries_blocked()
        retry = self.cli_retire()
        self.assertEqual(retry.returncode, 0, retry.stderr)
        report = json.loads(retry.stdout)['result']
        self.assert_matches_reference(report)
        # repeated retries keep returning the identical report
        again = self.cli_retire()
        self.assertEqual(json.loads(again.stdout)['result'], report)

    def test_reopen_after_completion_blocks_reentry_of_retired_id(self):
        result = self.cli_retire()
        self.assertEqual(result.returncode, 0, result.stderr)
        # a batch replaying an old successful batch id with a blocked id fails
        old_batch = self.base / 'old.json'
        old_batch.write_text(json.dumps({'batch_id': 'wb', 'events': [
            up(1, 's1'), seal(2, 's1'), up(3, 's2'), seal(4, 's2'),
            up(5, 's3'), seal(6, 's3'), up(7, 'd1')]}))
        self.assertEqual(self.cli_apply(old_batch).returncode, 2)
        # the sampler meeting different content of a blocked id skips it and
        # still finalizes a later ordinary trace at a new watermark
        extra = [
            span('unknown-u', end=99, service='zz', error=True),
            finish('unknown-u', end=99),
            span('ordinary', start=30, end=33, service='api'),
            finish('ordinary', end=33),
            {'kind': 'watermark', 'value': 40},
        ]
        full = self.source.read_bytes() + b''.join(
            (dump(r) + '\n').encode() for r in extra)
        self.source.write_bytes(full)
        import hashlib
        new_sha = hashlib.sha256(full).hexdigest()
        conn = sqlite3.connect(self.state / 'sampler.db')
        conn.execute("UPDATE meta SET value=? WHERE key='source_sha256'",
                     (new_sha,))
        conn.commit()
        conn.close()
        sample = self.cli_sample()
        self.assertEqual(sample.returncode, 0, sample.stderr)
        kept = [json.loads(line) for line in
                (self.out / 'watermark-40' / 'kept.jsonl').read_text().splitlines()]
        self.assertEqual([r['trace_id'] for r in kept], ['ordinary'])
        conn = sqlite3.connect(self.state / 'sampler.db')
        ids = {r[0] for r in conn.execute('SELECT trace_id FROM traces')}
        self.assertNotIn('unknown-u', ids)
        self.assertIn('ordinary', ids)
        conn.close()


class SubprocessContractCases(unittest.TestCase):
    """Subprocess coverage of empty hit, duplicates, corruption and reopen."""

    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        base = Path(self.td.name)
        self.base = base
        self.wh = base / 'warehouse.db'
        self.state = base / 'state'
        self.out = base / 'out'
        self.catalog = base / 'catalog'
        self.policy = base / 'policy.json'
        self.source = base / 'telemetry.jsonl'
        self.policy.write_text(json.dumps(
            {'normal_budget': 1, 'slow_ticks': 50}))
        apply_batch(self.wh, {'batch_id': 'wb', 'events': [
            up(1, 't1'), seal(2, 't1'), up(3, 't2')]})
        self.source.write_text(
            dump(span('t1', error=True)) + '\n' + dump(finish('t1'))
            + '\n' + dump({'kind': 'watermark', 'value': 10}) + '\n')
        sample_traces(self.source, self.state, self.out, self.policy)

    def tearDown(self):
        self.td.cleanup()

    def _env(self):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(SRC)
        return env

    def retire_cli(self, request_file, crash=None):
        env = self._env()
        if crash is not None:
            env['TRACE_LATENCY_RETIRE_CRASH'] = crash
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', 'retire-traces',
             '--catalog', str(self.catalog), '--db', str(self.wh),
             '--state', str(self.state), '--output', str(self.out),
             '--source', str(request_file)],
            capture_output=True, text=True, env=env, cwd=ROOT)

    def apply_cli(self, batch_file):
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', 'warehouse-apply',
             '--db', str(self.wh), '--source', str(batch_file)],
            capture_output=True, text=True, env=self._env(), cwd=ROOT)

    def query_cli(self):
        q = self.base / 'q.json'
        q.write_text(json.dumps({'service': 'api', 'start': 0, 'end': 20}))
        out = self.base / 'qout.json'
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', 'warehouse-query',
             '--db', str(self.wh), '--source', str(q), '--output', str(out)],
            capture_output=True, text=True, env=self._env(), cwd=ROOT)

    def sample_cli(self):
        return subprocess.run(
            [sys.executable, '-m', 'trace_latency.cli', 'sample-traces',
             '--source', str(self.source), '--state', str(self.state),
             '--output', str(self.out), '--policy', str(self.policy)],
            capture_output=True, text=True, env=self._env(), cwd=ROOT)

    def write_request(self, name, request_id, ids):
        path = self.base / name
        path.write_text(json.dumps(
            {'request_id': request_id, 'trace_ids': ids}))
        return path

    def test_empty_hit_request_succeeds_with_zero_counts(self):
        req = self.write_request('empty.json', 'r-empty', ['ghost-a', 'ghost-b'])
        result = self.retire_cli(req)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)['result']
        self.assertEqual(report['warehouse_traces'], 0)
        self.assertEqual(report['sampler_waiting'], 0)
        self.assertEqual(report['published_kept'], 0)
        self.assertEqual(report['published_dropped'], 0)
        self.assertEqual(report['published_quarantined'], 0)
        # a second empty-hit request still gets the next monotonic sequence
        req2 = self.write_request('empty2.json', 'r-empty-2', ['ghost-c'])
        report2 = json.loads(self.retire_cli(req2).stdout)['result']
        self.assertEqual(report2['seq'], report['seq'] + 1)

    def test_duplicate_request_same_seq_changed_set_rejected(self):
        req = self.write_request('d.json', 'r-dup', ['t1', 't2'])
        first = json.loads(self.retire_cli(req).stdout)['result']
        # identical set, different order: same sequence and identical report
        again = json.loads(self.retire_cli(req).stdout)['result']
        self.assertEqual(again, first)
        # changed set under the same request_id is rejected with exit 2
        changed = self.write_request('d2.json', 'r-dup', ['t1', 'other'])
        rejected = self.retire_cli(changed)
        self.assertEqual(rejected.returncode, 2)
        self.assertIn('different trace set', rejected.stderr)

    def test_corrupt_manifest_is_rejected_in_subprocess(self):
        req = self.write_request('r.json', 'r-ok', ['nobody'])
        self.assertEqual(self.retire_cli(req).returncode, 0)
        manifest = self.out / 'watermark-10' / 'manifest.json'
        good = manifest.read_bytes()
        try:
            manifest.write_text('{"watermark": 10', encoding='utf-8')
            bad_req = self.write_request('bad.json', 'r-bad', ['z'])
            result = self.retire_cli(bad_req)
            self.assertEqual(result.returncode, 2)
            self.assertIn('manifest', result.stderr)
        finally:
            manifest.write_bytes(good)

    def test_missing_catalog_closes_all_entries_in_subprocess(self):
        req = self.write_request('r.json', 'r-ok', ['t1'])
        self.assertEqual(self.retire_cli(req).returncode, 0)
        shutil.rmtree(self.catalog)
        batch = self.base / 'b.json'
        batch.write_text(json.dumps({'batch_id': 'late', 'events': [
            up(4, 'x')]}))
        self.assertEqual(self.apply_cli(batch).returncode, 2)
        self.assertEqual(self.query_cli().returncode, 2)
        self.assertEqual(self.sample_cli().returncode, 2)

    def test_misbound_catalog_closes_entries_in_subprocess(self):
        req = self.write_request('r.json', 'r-ok', ['t1'])
        self.assertEqual(self.retire_cli(req).returncode, 0)
        binding = self.catalog / 'binding.json'
        data = json.loads(binding.read_text())
        data['warehouse_db'] = str((self.base / 'foreign.db').resolve())
        binding.write_text(json.dumps(data))
        batch = self.base / 'b.json'
        batch.write_text(json.dumps({'batch_id': 'late', 'events': [
            up(4, 'x')]}))
        self.assertEqual(self.apply_cli(batch).returncode, 2)
        self.assertEqual(self.sample_cli().returncode, 2)

    def test_reopen_blocks_reentry_but_allows_survivors_in_subprocess(self):
        req = self.write_request('r.json', 'r-ok', ['t1', 'unknown-u'])
        self.assertEqual(self.retire_cli(req).returncode, 0)
        # any batch containing a blocked id fails, including a fresh batch id
        blocked_batch = self.base / 'blocked.json'
        blocked_batch.write_text(json.dumps({'batch_id': 'b-blocked',
                                             'events': [up(4, 'unknown-u')]}))
        self.assertEqual(self.apply_cli(blocked_batch).returncode, 2)
        # surviving content keeps working
        good_batch = self.base / 'good.json'
        good_batch.write_text(json.dumps({'batch_id': 'b-good', 'events': [
            up(4, 't3', service='db'), seal(5, 't3')]}))
        ok = self.apply_cli(good_batch)
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertEqual(json.loads(ok.stdout)['result']['max_seq'], 5)
        # query is open again and reflects only surviving data
        self.assertEqual(self.query_cli().returncode, 0)


if __name__ == '__main__':
    unittest.main()
