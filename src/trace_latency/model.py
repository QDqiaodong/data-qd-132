from .common import require,ident,number,rows
from .intervals import breakdown

def analyze(data):
    require(data,'trace_id spans');trace=ident(data['trace_id']);spans={}
    for s in rows(data['spans'],5000):
        require(s,'id parent start end');sid=ident(s['id']);parent=None if s['parent'] is None else ident(s['parent'])
        start=number(s['start'],0,10**12);end=number(s['end'],0,10**12)
        if sid in spans or end<start:raise ValueError('duplicate span or reversed interval')
        spans[sid]={'parent':parent,'start':start,'end':end}
    roots=[s for s in spans if spans[s]['parent'] is None]
    if len(roots)!=1:raise ValueError('exactly one root required')
    children={s:[] for s in spans}
    for sid,s in spans.items():
        p=s['parent']
        if p is None:continue
        if p not in spans:raise ValueError('unknown parent')
        if not spans[p]['start']<=s['start']<=s['end']<=spans[p]['end']:raise ValueError('child outside parent interval')
        children[p].append(sid)
    seen=set();pending=[roots[0]]
    while pending:
        sid=pending.pop()
        if sid in seen:raise ValueError('span cycle')
        seen.add(sid);pending.extend(children[sid])
    if len(seen)!=len(spans):raise ValueError('disconnected cycle')
    return {'trace_id':trace,**breakdown(spans,children,roots[0])}
