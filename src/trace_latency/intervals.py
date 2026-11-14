def union_length(intervals):
    total=0;left=right=None
    for a,b in sorted(intervals):
        if left is None:left,right=a,b
        elif a<=right:right=max(right,b)
        else:total+=right-left;left,right=a,b
    return total+(right-left if left is not None else 0)

def breakdown(spans,children,root):
    result=[]
    for sid,s in sorted(spans.items()):
        duration=s['end']-s['start'];covered=union_length([(spans[c]['start'],spans[c]['end']) for c in children[sid]])
        result.append({'span':sid,'parent':s['parent'],'inclusive_ticks':duration,'direct_child_union_ticks':covered,'exclusive_ticks':duration-covered})
    return {'root':root,'wall_ticks':spans[root]['end']-spans[root]['start'],'sum_span_ticks':sum(s['end']-s['start'] for s in spans.values()),'spans':result}
