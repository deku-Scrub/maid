import os
import sys
import importlib
import argparse
import logging

import maid


def get_cmdline_args():
    parser = argparse.ArgumentParser(
            description='maid description',
            )

    parser.add_argument(
            'task',
            type=str,
            nargs='*',
            help='task help',
            )

    parser.add_argument(
            '-f',
            '--file',
            dest='filename',
            default='Maidfile.py',
            type=str,
            help='filename help',
            )
    parser.add_argument(
            '-n',
            '--dry-run',
            dest='dry_run',
            action='store_true',
            default=False,
            help='dry-run help',
            )
    parser.add_argument(
            '-v',
            '--verbose',
            dest='verbose',
            action='store_true',
            default=False,
            help='verbose help',
            )
    parser.add_argument(
            '--hash',
            dest='use_hash',
            default=False,
            action='store_true',
            help='hash help',
            )
    parser.add_argument(
            '-k',
            '--keep-going',
            dest='finish_depth_on_failure',
            action='store_true',
            default=False,
            help='keep_going help',
            )
    parser.add_argument(
            '-t',
            '--touch',
            dest='update_requested',
            action='store_true',
            default=False,
            help='touch help',
            )

    # TODO: implement these.  Requires a global config.
    parser.add_argument(
            '--shell',
            dest='shell',
            default='bash -c',
            type=str,
            help='shell help',
            )
    parser.add_argument(
            '--no-print-script-output',
            dest='print_script_output',
            default=True,
            action='store_false',
            help='no_print_script_output help',
            )
    parser.add_argument(
            '--no-print-script',
            dest='print_script',
            default=True,
            action='store_false',
            help='no_print_script help',
            )
    parser.add_argument(
            '-q',
            '--quiet',
            dest='quiet',
            default=False,
            action='store_true',
            help='quiet help',
            )
    parser.add_argument(
            '-j',
            '--jobs',
            dest='n_jobs',
            default=1,
            type=int,
            help='jobs help',
            )

    return parser.parse_args()


def main():
    args = get_cmdline_args()
    print(args)

    if not os.path.exists(args.filename):
        print('File not found: `{}`.'.format(args.filename), file=sys.stderr)
        exit(1)

    log_level = logging.DEBUG if args.verbose else logging.CRITICAL
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(module)s - %(filename)s - %(message)s', level=log_level)

    sys.path.append(os.path.dirname(args.filename))
    module, _ = os.path.splitext(os.path.basename(args.filename))
    importlib.import_module(module)

    if 'help' in args.task:
        maid.describe_tasks()
        return
    if 'maids' in args.task:
        maid.describe_maids()
        return

    for task in ([''] if not args.task else args.task):
        workflow_id = task.split(':')
        task, maid_name = workflow_id if len(workflow_id) == 2 else (task, 'Default maid')
        if not maid.exists(maid_name):
            print('Maid `{}` does not exist.'.format(maid_name))
            exit(1)
        cur_maid = maid.get_maid(maid_name)
        cur_maid.work(
                task,
                dry_run=args.dry_run,
                use_hash=args.use_hash,
                update_requested=args.update_requested,
                finish_depth_on_failure=args.finish_depth_on_failure,
                )


if __name__ == '__main__':
    main()
