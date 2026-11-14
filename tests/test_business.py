
import copy,json,unittest,tempfile,subprocess,sys,hashlib
from pathlib import Path
from trace_latency.model import analyze
ROOT=Path(__file__).resolve().parents[1]
def sample():return json.loads((ROOT/'fixtures/input.json').read_text())
class Business(unittest.TestCase):
    def bad(self,d):
        with self.assertRaises((ValueError,TypeError)):analyze(d)
    def test_unknown_top_field(self):
        d=sample();d['unexpected']=1;self.bad(d)

    def test_overlap(self):
        r=analyze(sample());s={x['span']:x for x in r['spans']};self.assertEqual((r['wall_ticks'],r['sum_span_ticks']),(20,39));self.assertEqual((s['root']['direct_child_union_ticks'],s['root']['exclusive_ticks'],s['A']['exclusive_ticks']),(13,7,5))
    def test_order(self):
        d=sample();d['spans'].reverse();self.assertEqual(analyze(d),analyze(sample()))
    def test_zero(self):
        d={'trace_id':'x','spans':[{'id':'r','parent':None,'start':2,'end':2}]};self.assertEqual(analyze(d)['wall_ticks'],0)
    def test_nested_siblings(self):
        d=sample();d['spans'][2].update(start=4,end=8);r=analyze(d);self.assertEqual(next(x for x in r['spans'] if x['span']=='root')['exclusive_ticks'],12)
    def test_outside(self):
        d=sample();d['spans'][1]['end']=21;self.bad(d)
    def test_two_roots(self):
        d=sample();d['spans'][1]['parent']=None;self.bad(d)
    def test_disconnected_cycle(self):
        d=sample();d['spans'] += [{'id':'x','parent':'y','start':0,'end':0},{'id':'y','parent':'x','start':0,'end':0}];self.bad(d)
    def test_touching_union(self):
        from trace_latency.intervals import union_length
        self.assertEqual(union_length([(0,2),(2,4),(6,9)]),7)

class Command(unittest.TestCase):
    def call(self,source,output):
        return subprocess.run([sys.executable,'-m','trace_latency.cli','--source',str(source),'--output',str(output)],capture_output=True,text=True)
    def test_installed_cli_replay(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'out.json';src=ROOT/'fixtures/input.json';r=self.call(src,out);self.assertEqual(r.returncode,0,r.stderr);first=out.read_bytes();report=json.loads(first);self.assertEqual(report['source_sha256'],hashlib.sha256(src.read_bytes()).hexdigest());self.assertEqual(self.call(src,out).returncode,0);self.assertEqual(first,out.read_bytes())
    def test_failure_preserves_output(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'bad.json';out=Path(td)/'out.json';out.write_text('prior')
            for body in ['{}','{"x":1,"x":2}','NaN']:
                src.write_text(body);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(out.read_text(),'prior')
    def test_output_safety(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'in.json';src.write_bytes((ROOT/'fixtures/input.json').read_bytes());before=src.read_bytes();self.assertEqual(self.call(src,src).returncode,2);self.assertEqual(src.read_bytes(),before);out=Path(td)/'link.json';out.symlink_to(src);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(src.read_bytes(),before)
