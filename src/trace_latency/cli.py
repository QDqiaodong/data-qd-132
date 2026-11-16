DESCRIPTION='Nested trace validation, exclusive latency accounting and SQLite warehouse'
import argparse, json, sqlite3, sys
from pathlib import Path
from .common import read, save
from .model import analyze
from . import warehouse
from . import sampler
from . import retirement
from . import slo


def run(source):
    data, digest = read(source)
    return {'schema_version': 1, 'source_sha256': digest, 'result': analyze(data)}


def legacy_parser():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    return parser


def command_parser():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    sub = parser.add_subparsers(dest='command', required=True)
    applied = sub.add_parser('warehouse-apply',
                             help='apply one revision batch to a SQLite warehouse')
    applied.add_argument('--db', type=Path, required=True)
    applied.add_argument('--source', type=Path, required=True)
    applied.add_argument('--output', type=Path, default=None)
    queried = sub.add_parser('warehouse-query',
                             help='query service occupancy from a SQLite warehouse')
    queried.add_argument('--db', type=Path, required=True)
    queried.add_argument('--source', type=Path, required=True)
    queried.add_argument('--output', type=Path, required=True)
    sampled = sub.add_parser('sample-traces',
                             help='collect out-of-order trace JSONL into a '
                                  'recoverable tail-sampling dataset')
    sampled.add_argument('--source', type=Path, required=True)
    sampled.add_argument('--state', type=Path, required=True)
    sampled.add_argument('--output', type=Path, required=True)
    sampled.add_argument('--policy', type=Path, required=True)
    retired = sub.add_parser('retire-traces',
                             help='retire a bounded set of trace ids from one '
                                  'catalog-bound warehouse/sampler resource set')
    retired.add_argument('--catalog', type=Path, required=True)
    retired.add_argument('--db', type=Path, required=True)
    retired.add_argument('--state', type=Path, required=True)
    retired.add_argument('--output', type=Path, required=True)
    retired.add_argument('--source', type=Path, required=True)
    sinit = sub.add_parser('slo-init',
                           help='enable the persistent SLO alert pipeline on a warehouse')
    sinit.add_argument('--db', type=Path, required=True)
    sinit.add_argument('--source', type=Path, required=True)
    sstat = sub.add_parser('slo-status',
                           help='read an SLO snapshot (alert states and unacked count)')
    sstat.add_argument('--db', type=Path, required=True)
    sclaim = sub.add_parser('slo-claim',
                            help='claim unacked SLO notifications with a lease')
    sclaim.add_argument('--db', type=Path, required=True)
    sclaim.add_argument('--source', type=Path, required=True)
    sack = sub.add_parser('slo-ack',
                          help='acknowledge claimed SLO notifications atomically')
    sack.add_argument('--db', type=Path, required=True)
    sack.add_argument('--source', type=Path, required=True)
    return parser


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ('warehouse-apply', 'warehouse-query',
                            'sample-traces', 'retire-traces',
                            'slo-init', 'slo-status', 'slo-claim', 'slo-ack'):
        args = command_parser().parse_args(argv)
    else:
        args = legacy_parser().parse_args(argv)
        args.command = None
    try:
        if args.command == 'retire-traces':
            result = retirement.retire_traces(
                args.catalog, args.db, args.state, args.output, args.source)
            report = {'schema_version': 4, 'kind': 'retire_traces',
                      'result': result}
        elif args.command == 'slo-init':
            data, digest = read(args.source)
            result = slo.slo_init(args.db, data)
            report = {'schema_version': 5, 'kind': 'slo_init',
                      'source_sha256': digest, 'result': result}
        elif args.command == 'slo-status':
            result = slo.slo_status(args.db)
            report = {'schema_version': 5, 'kind': 'slo_status',
                      'result': result}
        elif args.command == 'slo-claim':
            data, digest = read(args.source)
            result = slo.slo_claim(args.db, data)
            report = {'schema_version': 5, 'kind': 'slo_claim',
                      'source_sha256': digest, 'result': result}
        elif args.command == 'slo-ack':
            data, digest = read(args.source)
            result = slo.slo_ack(args.db, data)
            report = {'schema_version': 5, 'kind': 'slo_ack',
                      'source_sha256': digest, 'result': result}
        elif args.command == 'sample-traces':
            report = sampler.sample_traces(args.source, args.state,
                                           args.output, args.policy)
        elif args.command == 'warehouse-apply':
            data, digest = read(args.source)
            result = warehouse.apply_batch(args.db, data)
            report = {'schema_version': 2, 'kind': 'warehouse_apply',
                      'source_sha256': digest, 'result': result}
            if args.output is not None:
                if args.source.resolve() == args.output.resolve():
                    raise ValueError('input and output must differ')
                save(args.output, report)
        elif args.command == 'warehouse-query':
            data, digest = read(args.source)
            result = warehouse.query(args.db, data)
            report = {'schema_version': 2, 'kind': 'warehouse_query',
                      'source_sha256': digest, 'result': result}
            if args.source.resolve() == args.output.resolve():
                raise ValueError('input and output must differ')
            save(args.output, report)
        else:
            if args.source.resolve() == args.output.resolve():
                raise ValueError('input and output must differ')
            report = run(args.source)
            save(args.output, report)
    except (ValueError, TypeError, KeyError, OSError, OverflowError,
            RecursionError, json.JSONDecodeError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(report, sort_keys=True, ensure_ascii=False,
                     allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
