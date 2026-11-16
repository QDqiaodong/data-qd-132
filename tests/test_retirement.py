"""Tests for targeted trace retirement (catalog, control plane and gates)."""
import hashlib
import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from trace_latency import apply_batch, sample_traces, retire_traces
from trace_latency.warehouse import query
from trace_latency.retirement import (
    RetirementError, validate_request, request_set_digest, OUTPUT_MARKER,
    CATALOG_META_KEY, CONTROL_DB,
)


def up(seq, tid, sid='r', parent=None, start=0, end=10, service='api'):
    return {'seq': seq, 'trace_id': tid, 'op': 'upsert',
            'span': {'id': sid, 'parent': parent, 'start': start, 'end': end,
                     'service': service}}


def seal(seq, tid):
    return {'seq': seq, 'trace_id': tid, 'op': 'seal'}


def rec_span(tid, sid='r', parent=None, start=0, end=10, service='api',
             error=False):
    return {'kind': 'span', 'trace_id': tid,
            'span': {'id': sid, 'parent': parent, 'start': start, 'end': end,
                     'service': service, 'error': error}}


def rec_finish(tid, root='r', end=10, count=1):
    return {'kind': 'finish', 'trace_id': tid, 'root_id': root, 'end': end,
            'span_count': count}


def rec_wm(value):
    return {'kind': 'watermark', 'value': value}


def dump(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))


class RequestValidation(unittest.TestCase):
    def bad(self, data):
        with self.assertRaises((RetirementError, ValueError, TypeError)):
            validate_request(data)

    def test_strict_shape(self):
        self.bad({})
        self.bad({'request_id': 'r'})
        self.bad({'trace_ids': ['t']})
        self.bad({'request_id': 'r', 'trace_ids': []})
        self.bad({'request_id': 'r', 'trace_ids': ['t'], 'extra': 1})
        self.bad({'request_id': 1, 'trace_ids': ['t']})
        self.bad({'request_id': 'r', 'trace_ids': 't'})
        self.bad({'request_id': 'r', 'trace_ids': ['']})
        self.bad({'request_id': ' r', 'trace_ids': ['t']})

    def test_distinct_and_limit(self):
        self.bad({'request_id': 'r', 'trace_ids': ['a', 'a']})
        ids = ['t%05d' % i for i in range(10_001)]
        self.bad({'request_id': 'r', 'trace_ids': ids})
        rid, ordered = validate_request(
            {'request_id': 'r', 'trace_ids': ['z', 'a', '中']})
        self.assertEqual(rid, 'r')
        # normalised by UTF-8 byte order
        self.assertEqual(ordered, sorted(['z', 'a', '中'],
                                         key=lambda x: x.encode('utf-8')))
        # exactly 10 000 is accepted and order independent
        ok_ids = ['t%05d' % i for i in range(10_000)]
        _, one = validate_request({'request_id': 'r', 'trace_ids': ok_ids})
        _, two = validate_request(
            {'request_id': 'r', 'trace_ids': list(reversed(ok_ids))})
        self.assertEqual(request_set_digest(one), request_set_digest(two))


class ManagedFixture(unittest.TestCase):
    """Builds one catalog-bound warehouse + sampler resource set per test."""

    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        base = Path(self.td.name)
        self.base = base
        self.wh = base / 'warehouse.db'
        self.state = base / 'sample-state'
        self.out = base / 'sample-out'
        self.catalog = base / 'catalog'
        self.policy = base / 'policy.json'
        self.source = base / 'telemetry.jsonl'
        self.policy.write_text(json.dumps(
            {'normal_budget': 1, 'slow_ticks': 50}))
        self._build_resources()

    def tearDown(self):
        self.td.cleanup()

    def _build_resources(self):
        # Warehouse: t-sealed (sealed), t-draft (draft only).
        apply_batch(self.wh, {'batch_id': 'wb-1', 'events': [
            up(1, 't-sealed', end=10, service='api'),
            seal(2, 't-sealed'),
            up(3, 't-draft', end=8, service='db'),
        ]})
        # Sampler at watermark 10: t-kept (error, kept under api), t-drop
        # and t-drop-rival are normal traces sharing root service svc-drop,
        # so the budget of one keeps the digest winner and drops the rival;
        # t-quar is quarantined and t-wait is still waiting at EOF.
        # t-survive is a normal kept trace in service db that survives.
        records = [
            rec_span('t-kept', error=True),
            rec_finish('t-kept'),
            rec_span('t-drop-rival', end=4, service='svc-drop'),
            rec_finish('t-drop-rival', end=4),
            rec_span('t-drop', end=4, service='svc-drop'),
            rec_finish('t-drop', end=4),
            rec_span('t-survive', end=6, service='db'),
            rec_finish('t-survive', end=6),
            rec_span('t-quar', end=5),
            rec_finish('t-quar', root='ghost', end=5),
            rec_wm(10),
            rec_span('t-wait', end=9),
        ]
        self.source.write_text(
            ''.join(dump(r) + '\n' for r in records), encoding='utf-8')
        self.sampler_report = sample_traces(
            self.source, self.state, self.out, self.policy)
        self.assertEqual(self.sampler_report['pending'], 1)
        self.assertEqual(self.sampler_report['kept'], 3)
        self.assertEqual(self.sampler_report['dropped'], 1)
        self.assertEqual(self.sampler_report['quarantined'], 1)
        # remember which of the svc-drop pair was dropped
        dropped_ids = [r['trace_id'] for r in
                       self.read_jsonl(10, 'dropped.jsonl')]
        self.assertEqual(len(dropped_ids), 1)
        self.drop_id = dropped_ids[0]
        self.kept_ids = [r['trace_id'] for r in
                         self.read_jsonl(10, 'kept.jsonl')]

    def write_request(self, name, request_id, ids):
        path = self.base / name
        path.write_text(json.dumps({'request_id': request_id,
                                    'trace_ids': ids}), encoding='utf-8')
        return path

    def retire(self, request_id, ids, name='request.json'):
        path = self.write_request(name, request_id, ids)
        return retire_traces(self.catalog, self.wh, self.state, self.out, path)

    def wm_path(self, value, filename):
        return self.out / ('watermark-%d' % value) / filename

    def read_jsonl(self, value, filename):
        return [json.loads(line) for line in
                self.wm_path(value, filename).read_text().splitlines()]

    def wh_conn(self):
        return sqlite3.connect(self.wh)

    def sm_conn(self):
        return sqlite3.connect(self.state / 'sampler.db')

    def ctl_conn(self):
        return sqlite3.connect(self.catalog / CONTROL_DB)


class CatalogInit(ManagedFixture):
    def test_first_run_creates_binding_and_claims(self):
        report = self.retire('r-1', ['ghost-only'])
        self.assertEqual(report['seq'], 1)
        binding = json.loads(
            (self.catalog / 'binding.json').read_text(encoding='utf-8'))
        self.assertEqual(binding['version'], 1)
        self.assertEqual(binding['warehouse_db'], str(self.wh.resolve()))
        self.assertEqual(binding['sampler_db'],
                         str((self.state / 'sampler.db').resolve()))
        self.assertEqual(binding['output_root'], str(self.out.resolve()))
        self.assertEqual([w['watermark'] for w in binding['watermarks']], [10])
        wh = self.wh_conn()
        self.assertEqual(wh.execute(
            "SELECT value FROM meta WHERE key=?", (CATALOG_META_KEY,)).fetchone()[0],
            str(self.catalog.resolve()))
        wh.close()
        sm = self.sm_conn()
        self.assertEqual(sm.execute(
            "SELECT value FROM meta WHERE key=?", (CATALOG_META_KEY,)).fetchone()[0],
            str(self.catalog.resolve()))
        sm.close()
        self.assertEqual(
            (self.out / OUTPUT_MARKER).read_text().strip(),
            str(self.catalog.resolve()))

    def test_reopen_with_same_paths_is_idempotent(self):
        self.retire('r-1', ['ghost-only'])
        before = (self.catalog / 'binding.json').read_bytes()
        report = self.retire('r-2', ['other-ghost'])
        self.assertEqual(report['seq'], 2)
        self.assertEqual((self.catalog / 'binding.json').read_bytes(), before)

    def test_different_paths_rejected(self):
        self.retire('r-1', ['ghost-only'])
        other = self.write_request('o.json', 'r-x', ['z'])
        with self.assertRaises(RetirementError):
            retire_traces(self.catalog, self.base / 'other.db', self.state,
                          self.out, other)
        with self.assertRaises(RetirementError):
            retire_traces(self.catalog, self.wh,
                          self.base / 'other-state', self.out, other)
        with self.assertRaises(RetirementError):
            retire_traces(self.catalog, self.wh, self.state,
                          self.base / 'other-out', other)

    def test_alias_path_binds_same_canonical_resource(self):
        # A symlink alias must resolve to the same canonical path rather than
        # letting one resource join under two names.
        link = self.base / 'wh-alias.db'
        link.symlink_to(self.wh)
        path = self.write_request('a.json', 'r-a', ['g'])
        report = retire_traces(self.catalog, link, self.state, self.out, path)
        self.assertEqual(report['status'], 'done')

    def test_source_cannot_be_a_target(self):
        bad = self.base / 'inside'
        bad.mkdir()
        target_req = bad / 'request.json'
        target_req.write_text(json.dumps({'request_id': 'r', 'trace_ids': ['x']}))
        with self.assertRaises(RetirementError):
            retire_traces(self.catalog, target_req, self.state, self.out,
                          target_req)
        with self.assertRaises(RetirementError):
            retire_traces(self.base, self.wh, self.state, self.out,
                          self.write_request('q.json', 'r', ['x']))

    def test_cross_catalog_rejected(self):
        self.retire('r-1', ['ghost-only'])
        second = self.base / 'catalog-two'
        path = self.write_request('c.json', 'r-c', ['z'])
        with self.assertRaises(RetirementError):
            retire_traces(second, self.wh, self.state, self.out, path)

    def test_missing_resources_rejected(self):
        fresh = self.base / 'fresh-cat'
        path = self.write_request('m.json', 'r-m', ['z'])
        with self.assertRaises(RetirementError):
            retire_traces(fresh, self.base / 'nope.db', self.state,
                          self.out, path)
        with self.assertRaises(RetirementError):
            retire_traces(fresh, self.wh, self.base / 'no-state',
                          self.out, path)
        with self.assertRaises(RetirementError):
            retire_traces(fresh, self.wh, self.state,
                          self.base / 'no-out', path)

    def test_corrupt_manifest_closes_init(self):
        # First retire to establish a valid catalog, then corrupt a manifest
        # and open a new catalog over the same output root: rejected.
        self.retire('r-1', ['ghost-only'])
        manifest = self.wm_path(10, 'manifest.json')
        good = manifest.read_bytes()
        manifest.write_text('{"watermark": 10', encoding='utf-8')
        path = self.write_request('d.json', 'r-d', ['z'])
        try:
            with self.assertRaises(RetirementError):
                retire_traces(self.base / 'cat-other', self.wh, self.state,
                              self.out, path)
        finally:
            manifest.write_bytes(good)

    def test_watermark_high_mark_must_match_sampler(self):
        # Tamper the sampler last_watermark so it disagrees with the output.
        sm = self.sm_conn()
        sm.execute("UPDATE meta SET value='99' WHERE key='last_watermark'")
        sm.commit()
        sm.close()
        path = self.write_request('wm.json', 'r-wm', ['z'])
        with self.assertRaises(RetirementError):
            retire_traces(self.catalog, self.wh, self.state, self.out, path)

    def test_interrupted_init_resumes_with_same_paths_only(self):
        # Simulate a kill after the warehouse claim but before the binding
        # file and other claims exist: no request can activate yet, and a
        # retry with the identical path set completes the binding.
        from trace_latency.retirement import _set_meta
        conn = self.wh_conn()
        conn.execute('BEGIN IMMEDIATE')
        _set_meta(conn, CATALOG_META_KEY, str(self.catalog.resolve()))
        conn.execute('COMMIT')
        conn.close()
        self.assertFalse((self.catalog / 'binding.json').exists())
        path = self.write_request('resume.json', 'r-resume', ['g'])
        report = retire_traces(self.catalog, self.wh, self.state, self.out,
                               path)
        self.assertEqual(report['status'], 'done')
        self.assertTrue((self.catalog / 'binding.json').is_file())
        # the partially claimed warehouse did not leak a partial control state
        ctl = self.ctl_conn()
        self.assertEqual(ctl.execute(
            "SELECT status FROM requests WHERE request_id='r-resume'").fetchone()[0],
            'done')
        ctl.close()

    def test_no_watermark_when_sampler_has_none(self):
        # A sampler with no published watermark has an empty output root;
        # binding succeeds and there is no watermark directory.
        empty_state = self.base / 'empty-state'
        empty_out = self.base / 'empty-out'
        empty_src = self.base / 'empty.jsonl'
        empty_src.write_bytes(b'')
        sample_traces(empty_src, empty_state, empty_out, self.policy)
        # the sampler created no watermark tree (only possibly nothing)
        self.assertEqual([p.name for p in empty_out.iterdir()
                          if not p.name.startswith('.')], [])
        path = self.write_request('e.json', 'r-empty', ['nobody'])
        report = retire_traces(self.base / 'empty-cat', self.wh,
                               empty_state, empty_out, path)
        self.assertEqual(report['published_kept'], 0)
        self.assertEqual(report['published_dropped'], 0)
        self.assertEqual(report['published_quarantined'], 0)
        self.assertEqual([p.name for p in empty_out.iterdir()],
                         [OUTPUT_MARKER])


class EndToEndRetirement(ManagedFixture):
    BASE_RETIRE = ['t-sealed', 't-draft', 't-kept', 't-quar', 't-wait',
                   'unknown-u']

    def setUp(self):
        super().setUp()
        # Snapshot bytes of files that must remain untouched.
        self.original_survivors = {
            'kept': self.wm_path(10, 'kept.jsonl').read_bytes(),
        }
        self.original_manifest = self.wm_path(10, 'manifest.json').read_bytes()
        # Retire both svc-drop rivals (one kept, one dropped) plus the other
        # targets; only t-survive should remain in the published dataset.
        self.RETIRE_IDS = self.BASE_RETIRE + ['t-drop', 't-drop-rival']
        self.report = self.retire('r-full', self.RETIRE_IDS)

    def test_report_counts(self):
        self.assertEqual(self.report, {
            'request_id': 'r-full',
            'seq': 1,
            'status': 'done',
            'trace_count': len(self.RETIRE_IDS),
            'warehouse_traces': 2,
            'sampler_waiting': 1,
            'published_kept': 2,
            'published_dropped': 1,
            'published_quarantined': 1,
        })

    def test_warehouse_payload_and_index_removed(self):
        conn = self.wh_conn()
        for table in ('traces', 'spans', 'contrib_endpoints', 'bit_index'):
            self.assertEqual(
                conn.execute('SELECT COUNT(*) FROM %s' % table).fetchone()[0],
                0, table)
        # batch idempotency evidence and max_seq survive
        self.assertEqual(
            conn.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 1)
        self.assertEqual(conn.execute(
            "SELECT value FROM meta WHERE key='max_seq'").fetchone()[0], '3')
        conn.close()
        result = query(self.wh, {'service': 'api', 'start': 0, 'end': 20})
        self.assertEqual(result['peak'], 0)
        self.assertEqual(result['max_seq'], 3)

    def test_sampler_state_removed_but_identity_offsets_kept(self):
        conn = self.sm_conn()
        # the surviving finalized trace keeps its finish row and per-span
        # duplicate evidence; every waiting trace and every span row is gone
        remaining = conn.execute('SELECT trace_id,status FROM traces').fetchall()
        self.assertEqual(remaining, [('t-survive', 'finalized')])
        self.assertEqual(
            conn.execute('SELECT COUNT(*) FROM spans').fetchone()[0], 0)
        evidence_ids = {r[0] for r in conn.execute(
            'SELECT DISTINCT trace_id FROM evidence')}
        self.assertEqual(evidence_ids, {'t-survive'})
        keys = dict(conn.execute('SELECT key,value FROM meta').fetchall())
        self.assertIn('source_sha256', keys)
        self.assertIn('policy_sha256', keys)
        self.assertEqual(keys['last_watermark'], '10')
        self.assertNotEqual(keys['offset'], '0')
        conn.close()

    def test_published_files_filtered_and_manifest_republished(self):
        kept = self.read_jsonl(10, 'kept.jsonl')
        self.assertEqual([r['trace_id'] for r in kept], ['t-survive'])
        self.assertEqual(self.read_jsonl(10, 'dropped.jsonl'), [])
        self.assertEqual(self.read_jsonl(10, 'quarantine.jsonl'), [])
        manifest = json.loads(
            self.wm_path(10, 'manifest.json').read_text(encoding='utf-8'))
        # source/policy file-level SHAs and the watermark survive
        self.assertEqual(manifest['watermark'], 10)
        self.assertEqual(manifest['source_sha256'],
                         hashlib.sha256(self.source.read_bytes()).hexdigest())
        self.assertEqual(manifest['policy_sha256'],
                         hashlib.sha256(self.policy.read_bytes()).hexdigest())
        for name, expected_count in (('kept.jsonl', 1), ('dropped.jsonl', 0),
                                     ('quarantine.jsonl', 0)):
            info = manifest['files'][name]
            raw = self.wm_path(10, name).read_bytes()
            self.assertEqual(info['sha256'], hashlib.sha256(raw).hexdigest())
            self.assertEqual(info['count'], expected_count)

    def test_survivor_line_bytes_unchanged(self):
        # The surviving record's original JSONL line is copied byte-for-byte
        # and stays the only line of the republished kept file.
        original_lines = self.original_survivors['kept'].splitlines(keepends=True)
        survivor_line = next(line for line in original_lines
                             if b't-survive' in line)
        self.assertEqual(self.wm_path(10, 'kept.jsonl').read_bytes(),
                         survivor_line)

    def test_control_rows_keep_only_allowed_evidence(self):
        conn = self.ctl_conn()
        status = conn.execute(
            "SELECT status FROM requests WHERE request_id='r-full'").fetchone()[0]
        self.assertEqual(status, 'done')
        blocked = {row[0] for row in conn.execute(
            'SELECT trace_id FROM blocked')}
        self.assertEqual(blocked, set(self.RETIRE_IDS))
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        # the control plane stores no span/service/timestamp tables at all
        self.assertEqual(tables - {'sqlite_sequence'},
                         {'requests', 'blocked', 'progress'})
        columns = {r[1] for r in conn.execute('PRAGMA table_info(requests)')}
        self.assertEqual(columns,
                         {'request_id', 'seq', 'set_sha256', 'trace_count',
                          'status', 'report_json'})
        conn.close()
        # business vocabulary must not leak into the control database bytes;
        # retired trace ids themselves are retained by design.
        control_dump = (self.catalog / CONTROL_DB).read_bytes()
        for forbidden in (b'service', b'span', b'"start"', b'"end"'):
            self.assertNotIn(forbidden, control_dump)

    def test_retry_returns_same_report_and_counts(self):
        again = self.retire('r-full', list(reversed(self.RETIRE_IDS)))
        self.assertEqual(again, self.report)

    def test_blocked_batch_rejected_including_old_batch_replay(self):
        original_batch = {'batch_id': 'wb-1', 'events': [
            up(1, 't-sealed', end=10, service='api'), seal(2, 't-sealed'),
            up(3, 't-draft', end=8, service='db')]}
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, {'batch_id': 'wb-new', 'events': [
                up(4, 'unknown-u')]})
        # even the exact replay of a previously successful batch id is
        # rejected wholesale because it mentions blocked ids
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, original_batch)
        # an untouched batch id on surviving content works normally
        ok = apply_batch(self.wh, {'batch_id': 'wb-2', 'events': [
            up(4, 't-fresh', service='api'), seal(5, 't-fresh')]})
        self.assertEqual(ok['max_seq'], 5)
        # replaying that surviving, non-blocked batch returns its original
        # sequence with replayed=true and writes nothing (old idempotency)
        replay = apply_batch(self.wh, {'batch_id': 'wb-2', 'events': [
            up(4, 't-fresh', service='api'), seal(5, 't-fresh')]})
        self.assertTrue(replay['replayed'])
        self.assertEqual((replay['seq_from'], replay['seq_to']), (4, 5))

    def test_sampler_skips_blocked_records_without_conflict_or_budget(self):
        # Simulate an archive that ships additional bytes after retirement:
        # the state consumed a prefix, then the read-only archive is replaced
        # by a full file whose prefix bytes are identical (its stored identity
        # is updated to the new whole-file SHA).  Records of an already
        # blocked id with *different* content must be skipped without a past
        # conflict, a trace row or budget use; the normal trace still ends.
        resume_records = [
            rec_span('unknown-u', sid='r', start=0, end=99, service='zz',
                     error=True),
            rec_finish('unknown-u', root='r', end=99, count=1),
            rec_span('normal-after', start=20, end=22, service='api'),
            rec_finish('normal-after', root='r', end=22, count=1),
            rec_wm(30),
        ]
        prefix_bytes = self.source.read_bytes()
        full_bytes = prefix_bytes + b''.join(
            (dump(r) + '\n').encode('utf-8') for r in resume_records)
        self.source.write_bytes(full_bytes)
        full_sha = hashlib.sha256(full_bytes).hexdigest()
        conn = self.sm_conn()
        conn.execute("UPDATE meta SET value=? WHERE key='source_sha256'",
                     (full_sha,))
        conn.commit()
        conn.close()
        report = sample_traces(self.source, self.state, self.out, self.policy)
        self.assertEqual(report['kept'], 1)
        conn = self.sm_conn()
        ids = {r[0] for r in conn.execute(
            'SELECT trace_id FROM traces')}
        self.assertIn('normal-after', ids)
        self.assertNotIn('unknown-u', ids)
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM spans WHERE trace_id='unknown-u'").fetchone()[0], 0)
        conn.close()

    def test_no_hit_directory_bytes_unchanged(self):
        # A second request that hits nothing must not rewrite any output byte.
        before = {p: p.read_bytes() for p in self.out.rglob('*')
                  if p.is_file()}
        self.retire('r-empty', ['nobody-home'])
        after = {p: p.read_bytes() for p in self.out.rglob('*')
                 if p.is_file()}
        self.assertEqual(before, after)

    def test_staging_directory_removed_after_completion(self):
        leftovers = [p for p in self.catalog.rglob('staging-*')]
        self.assertEqual(leftovers, [])


class RequestControl(ManagedFixture):
    def test_same_id_same_set_order_independent_same_seq(self):
        first = self.retire('r-1', ['a', 'b'], 'one.json')
        second = self.retire('r-1', ['b', 'a'], 'two.json')
        self.assertEqual(first['seq'], second['seq'])
        self.assertEqual(second, first)

    def test_same_id_changed_set_rejected(self):
        self.retire('r-1', ['a', 'b'])
        with self.assertRaises(RetirementError):
            self.retire('r-1', ['a', 'c'])

    def test_new_request_blocked_while_active(self):
        # Mark an active request directly in the control db, simulating a
        # maintenance process that died after acceptance but before cleanup.
        self.retire('r-done', ['z'])
        conn = self.ctl_conn()
        conn.execute(
            "INSERT INTO requests(request_id,seq,set_sha256,trace_count,status)"
            " VALUES('r-active',2,'digest',1,'active')")
        conn.execute("INSERT OR IGNORE INTO blocked VALUES('z2')")
        conn.commit()
        conn.close()
        with self.assertRaises(RetirementError):
            self.retire('r-new', ['q'])
        # all three entries are closed while active
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, {'batch_id': 'b', 'events': [up(1, 'x')]})
        with self.assertRaises(RetirementError):
            query(self.wh, {'service': 'api', 'start': 0, 'end': 1})
        with self.assertRaises(RetirementError):
            sample_traces(self.source, self.state, self.out, self.policy)

    def test_blocked_set_only_grows(self):
        self.retire('r-1', ['a', 'b'])
        self.retire('r-2', ['b', 'c'])
        conn = self.ctl_conn()
        blocked = {r[0] for r in conn.execute(
            'SELECT trace_id FROM blocked')}
        conn.close()
        self.assertEqual(blocked, {'a', 'b', 'c'})

    def test_monotonic_sequence(self):
        self.assertEqual(self.retire('r-1', ['a'])['seq'], 1)
        self.assertEqual(self.retire('r-2', ['b'])['seq'], 2)
        self.assertEqual(self.retire('r-3', ['c'])['seq'], 3)

    def test_unknown_trace_enters_blocked_set(self):
        self.retire('r-1', ['unknown-u'])
        conn = self.ctl_conn()
        self.assertTrue(conn.execute(
            'SELECT 1 FROM blocked WHERE trace_id=?',
            ('unknown-u',)).fetchone())
        conn.close()
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, {'batch_id': 'b-late', 'events': [
                up(4, 'unknown-u')]})

    def test_completed_retry_report_identical(self):
        first = self.retire('r-1', ['t-sealed', 't-kept'])
        second = self.retire('r-1', ['t-kept', 't-sealed'])
        self.assertEqual(first, second)
        conn = self.ctl_conn()
        self.assertEqual(conn.execute(
            'SELECT COUNT(*) FROM requests').fetchone()[0], 1)
        conn.close()


class GateBindingFailures(ManagedFixture):
    def test_missing_catalog_closes_entries(self):
        self.retire('r-1', ['g'])
        shutil.rmtree(self.catalog)
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, {'batch_id': 'b', 'events': [up(1, 'x')]})
        with self.assertRaises(RetirementError):
            query(self.wh, {'service': 'api', 'start': 0, 'end': 1})
        with self.assertRaises(RetirementError):
            sample_traces(self.source, self.state, self.out, self.policy)

    def test_missing_control_db_closes_entries(self):
        self.retire('r-1', ['g'])
        (self.catalog / CONTROL_DB).unlink()
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, {'batch_id': 'b', 'events': [up(1, 'x')]})

    def test_corrupt_control_db_closes_entries(self):
        self.retire('r-1', ['g'])
        (self.catalog / CONTROL_DB).write_bytes(b'not a sqlite database')
        with self.assertRaises((RetirementError, sqlite3.Error)):
            apply_batch(self.wh, {'batch_id': 'b', 'events': [up(1, 'x')]})
        with self.assertRaises((RetirementError, sqlite3.Error)):
            sample_traces(self.source, self.state, self.out, self.policy)

    def test_binding_mismatch_closes_entries(self):
        self.retire('r-1', ['g'])
        binding = self.catalog / 'binding.json'
        data = json.loads(binding.read_text())
        data['warehouse_db'] = str((self.base / 'foreign.db').resolve())
        binding.write_text(json.dumps(data))
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, {'batch_id': 'b', 'events': [up(1, 'x')]})

    def test_peer_claim_mismatch_closes_entries(self):
        self.retire('r-1', ['g'])
        conn = self.sm_conn()
        conn.execute("UPDATE meta SET value='/some/other/catalog' "
                     "WHERE key=?", (CATALOG_META_KEY,))
        conn.commit()
        conn.close()
        with self.assertRaises(RetirementError):
            apply_batch(self.wh, {'batch_id': 'b', 'events': [up(1, 'x')]})

    def test_output_marker_removed_closes_entries(self):
        self.retire('r-1', ['g'])
        (self.out / OUTPUT_MARKER).unlink()
        with self.assertRaises(RetirementError):
            sample_traces(self.source, self.state, self.out, self.policy)

    def test_unmanaged_resources_keep_old_contract(self):
        # Fresh, never-bound resources must behave exactly as before.
        fresh_wh = self.base / 'plain.db'
        result = apply_batch(fresh_wh, {'batch_id': 'b', 'events': [
            up(1, 't'), seal(2, 't')]})
        self.assertEqual(result['max_seq'], 2)
        self.assertEqual(query(fresh_wh, {
            'service': 'api', 'start': 0, 'end': 1})['peak'], 1)
        fresh_state = self.base / 'plain-state'
        fresh_out = self.base / 'plain-out'
        fresh_src = self.base / 'plain.jsonl'
        fresh_src.write_text(dump(rec_span('t')) + '\n' + dump(rec_finish('t'))
                             + '\n' + dump(rec_wm(10)) + '\n')
        report = sample_traces(fresh_src, fresh_state, fresh_out, self.policy)
        self.assertEqual(report['kept'], 1)


class MultiWatermarkSurvivorBytes(ManagedFixture):
    def test_other_watermark_untouched_and_order_kept(self):
        # Add a second watermark with two surviving records and one target.
        # New finish ends must exceed the already published watermark 10.
        extra = [
            rec_span('w2-keep-a', sid='a', start=10, end=12, service='api'),
            rec_finish('w2-keep-a', root='a', end=12, count=1),
            rec_span('w2-keep-b', sid='b', start=10, end=13, service='db'),
            rec_finish('w2-keep-b', root='b', end=13, count=1),
            # two normal traces share service svc-drop; the digest loser is
            # dropped and will be the target of retirement.
            rec_span('w2-drop-x', start=10, end=14, service='svc-drop'),
            rec_finish('w2-drop-x', end=14),
            rec_span('w2-drop-y', start=10, end=15, service='svc-drop'),
            rec_finish('w2-drop-y', end=15),
            rec_wm(20),
        ]
        # Recreate resources with both watermarks.
        shutil.rmtree(self.state)
        shutil.rmtree(self.out)
        base_records = [
            rec_span('w1-keep', error=True), rec_finish('w1-keep'),
            rec_span('w1-drop', end=4, service='svc-drop'),
            rec_finish('w1-drop', end=4),
            rec_wm(10),
        ] + extra
        self.source.write_text(
            ''.join(dump(r) + '\n' for r in base_records))
        self.sampler_report = sample_traces(
            self.source, self.state, self.out, self.policy)
        before = {p: p.read_bytes() for p in
                  (self.out / 'watermark-10').rglob('*') if p.is_file()}
        dropped = [r['trace_id'] for r in self.read_jsonl(20, 'dropped.jsonl')]
        self.assertEqual(len(dropped), 1)
        winner = 'w2-drop-y' if dropped[0] == 'w2-drop-x' else 'w2-drop-x'
        report = self.retire('r-multi', dropped)
        self.assertEqual(report['published_dropped'], 1)
        # watermark-10 bytes entirely unchanged
        after = {p: p.read_bytes() for p in
                 (self.out / 'watermark-10').rglob('*') if p.is_file()}
        self.assertEqual(before, after)
        kept = self.read_jsonl(20, 'kept.jsonl')
        self.assertEqual(sorted(r['trace_id'] for r in kept),
                         sorted(['w2-keep-a', 'w2-keep-b', winner]))
        manifest = json.loads(
            self.wm_path(20, 'manifest.json').read_text())
        self.assertEqual(manifest['files']['kept.jsonl']['count'], 3)
        self.assertEqual(manifest['files']['dropped.jsonl']['count'], 0)
        # source/policy SHAs remain the file-level identity values
        self.assertEqual(manifest['source_sha256'],
                         hashlib.sha256(self.source.read_bytes()).hexdigest())


class FenwickEquivalence(ManagedFixture):
    def test_contributions_equal_survivor_only_reference_warehouse(self):
        # Deterministic sealed traces occupy disjoint intervals per service,
        # so retiring a subset leaves histograms identical to a reference
        # warehouse that only ever received the surviving sealed traces.
        reference = self.base / 'reference.db'
        # The reference mirrors the shared fixture's surviving draft
        # (t-draft contributes nothing; the retired t-sealed is absent, as it
        # is after retirement).  max_seq history is irrelevant to histograms.
        apply_batch(reference, {'batch_id': 'wb-1', 'events': [
            up(1, 't-draft', end=8, service='db')]})
        events = []
        seq = 4
        rseq = 2
        ref_events = []
        for i in range(12):
            tid = 'tr%02d' % i
            service = 'api' if i % 2 else 'db'
            # disjoint intervals in a tick range the retired shared-fixture
            # t-sealed ([0,10) api) never reaches, so windows over this range
            # compare the rich traces only.
            start = 100 + i * 50
            end = start + 40
            events.append(up(seq, tid, start=start, end=end,
                             service=service)); seq += 1
            events.append(seal(seq, tid)); seq += 1
            if i % 3:                  # survivors: indices not divisible by 3
                ref_events.append(up(rseq, tid, start=start, end=end,
                                     service=service)); rseq += 1
                ref_events.append(seal(rseq, tid)); rseq += 1
        apply_batch(self.wh, {'batch_id': 'rich', 'events': events})
        apply_batch(reference, {'batch_id': 'ref', 'events': ref_events})
        retired = ['tr%02d' % i for i in range(12) if i % 3 == 0]
        self.retire('r-fen', retired + ['unknown-u'])
        for service in ('api', 'db'):
            for start, end in ((100, 700), (120, 300), (400, 700)):
                q = {'service': service, 'start': start, 'end': end}
                got = query(self.wh, q)
                want = query(reference, q)
                self.assertEqual(got['histogram'], want['histogram'], q)
                self.assertEqual(got['peak'], want['peak'], q)
                self.assertEqual(got['trace_ticks'], want['trace_ticks'], q)


class StreamingAndBoundedMemory(unittest.TestCase):
    """Retirement streams rows/lines; only the request set is fully held."""

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

    def tearDown(self):
        self.td.cleanup()

    def test_large_warehouse_retirement_stays_bounded(self):
        import resource
        # Many sealed traces in one warehouse; retire a subset and compare the
        # resident growth against a chunked baseline.
        total = 4000
        batch = []
        seq = 1
        for i in range(total):
            tid = 'w%05d' % i
            batch.append(up(seq, tid, start=i, end=i + 1,
                            service='api'))
            seq += 1
            batch.append(seal(seq, tid))
            seq += 1
            if len(batch) == 5000:
                apply_batch(self.wh,
                            {'batch_id': 'b%05d' % i, 'events': batch})
                batch = []
        if batch:
            apply_batch(self.wh, {'batch_id': 'blast', 'events': batch})
        # Sampler side: one small published watermark so the output exists.
        self.source.write_text(
            dump(rec_span('z', error=True)) + '\n' + dump(rec_finish('z'))
            + '\n' + dump(rec_wm(10)) + '\n')
        sample_traces(self.source, self.state, self.out, self.policy)
        # Retire the odd-numbered warehouse traces plus the sampler trace.
        retire_ids = ['w%05d' % i for i in range(1, total, 2)] + ['z', 'ghost']
        request = self.base / 'request.json'
        request.write_text(json.dumps(
            {'request_id': 'r-scale', 'trace_ids': retire_ids}))
        before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        report = retire_traces(self.catalog, self.wh, self.state, self.out,
                               request)
        used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before
        # The whole target set (10k cap) plus per-chunk streaming must fit
        # well under the 64 MiB working-memory budget.
        self.assertLess(used, 64 * 1024)
        self.assertEqual(report['warehouse_traces'], total // 2)
        self.assertEqual(report['published_kept'], 1)
        conn = sqlite3.connect(self.wh)
        remaining = conn.execute('SELECT COUNT(*) FROM traces').fetchone()[0]
        self.assertEqual(remaining, total - total // 2)
        # each surviving single-span sealed trace contributes exactly two
        # contribution endpoints; sparse Fenwick nodes may merge across
        # positions, so their count is only bounded by the endpoint count.
        self.assertEqual(
            conn.execute('SELECT COUNT(*) FROM contrib_endpoints').fetchone()[0],
            (total - total // 2) * 2)
        self.assertLessEqual(
            conn.execute('SELECT COUNT(*) FROM bit_index').fetchone()[0],
            (total - total // 2) * 2)
        # every batch idempotency row survives untouched (events are packed
        # into 5000-event batches, with any remainder in a final batch)
        expected_batches = (total * 2 + 4999) // 5000
        self.assertEqual(
            conn.execute('SELECT COUNT(*) FROM batches').fetchone()[0],
            expected_batches)
        conn.close()

    def test_output_total_larger_than_memory_streams_per_file(self):
        # A watermark with thousands of lines is filtered line-by-line; the
        # implementation never materialises the whole JSONL file.
        many = 8000
        records = []
        for i in range(many):
            records.append(rec_span('m%05d' % i, end=10))
            records.append(rec_finish('m%05d' % i, end=10))
        records.append(rec_wm(10))
        self.source.write_text(
            ''.join(dump(r) + '\n' for r in records))
        report = sample_traces(self.source, self.state, self.out, self.policy)
        self.assertEqual(report['kept'] + report['dropped'], many)
        # A warehouse must exist for catalog binding (minimal).
        apply_batch(self.wh, {'batch_id': 'b', 'events': [
            up(1, 'x'), seal(2, 'x')]})
        targets = ['m%05d' % i for i in range(0, many, 2)]
        request = self.base / 'request.json'
        request.write_text(json.dumps(
            {'request_id': 'r-stream', 'trace_ids': targets}))
        result = retire_traces(self.catalog, self.wh, self.state, self.out,
                               request)
        # each retired id landed in exactly one of kept/dropped
        self.assertEqual(result['published_kept']
                         + result['published_dropped'], len(targets))
        manifest = json.loads(
            (self.out / 'watermark-10' / 'manifest.json').read_text())
        remaining = sum(info['count'] for name, info in
                        manifest['files'].items() if name.endswith('.jsonl'))
        self.assertEqual(remaining, many - len(targets))


if __name__ == '__main__':
    unittest.main()
