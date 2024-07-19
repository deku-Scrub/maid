import sys
import os

import maid.graph
import maid.monitor.time
import maid.monitor.hash
import maid.error_utils


def _throw_missing_targets_exception(msg):
    '''
    '''
    raise MissingExpectedTargetsException(msg)


def _throw_if_depth_error(errors, depth, finish_depth_on_failure):
    if not errors:
        return

    if finish_depth_on_failure:
        if errors[0][0] < depth:
            raise errors[0][1]
    else:
        raise errors[0][1]


def _run_tasks(nodes, graph, workreq, finished_tasks=None):
    errors = []
    finished_tasks = finished_tasks if finished_tasks else set()

    for task_name, depth in nodes:
        _throw_if_depth_error(errors, depth, workreq.finish_depth_on_failure)

        task = graph[task_name]
        targets = task.get_targets()
        try:
            _ = task.run(modified_tasks=finished_tasks.intersection(task.required_tasks))
            if _is_any_target_not_found(task, targets):
                msg = 'Task `{task}` ran without error but did not create expected files'.format(task=task_name)
                raise MissingExpectedTargetsException(msg)
        except Exception as err:
            errors.append((depth, err))
            continue

        if workreq.use_hash:
            maid.monitor.hash.make_hashes(task.get_targets())
            maid.monitor.hash.make_hashes(task.get_required_files())

        finished_tasks.add(task_name)
    _throw_if_depth_error(errors, depth + 1, workreq.finish_depth_on_failure)


def _should_task_run(graph, use_hash=False):
    g = maid.monitor.time.should_task_run(graph)
    if use_hash:
        g = maid.monitor.hash.should_task_run(graph)

    def f(task_name):
        task = graph[task_name]
        if task.dont_run_if_all_targets_exist:
            # TODO: what to do when no targets?  `all` evaluates
            # to `True` when there are no targets.  Is this appropriate?
            if all(os.path.exists(f) for f in task.get_targets()):
                return tuple()
        if task.always_run_if_in_graph:
            return (task_name, '')

        return g(task_name)

    return f


def _throw_script_error(task, err_info, filenames):
    raise Exception('idk') # TODO: is this function necessary?


def _is_any_target_not_found(task, filenames):
    is_file_missing = False
    for filename in filenames:
        if not os.path.exists(filename):
            is_file_missing = True
            print('Task `{task}` ran without error but did not create an expected file: `{filename}` not found.'.format(task=task, filename=filename), file=sys.stderr)
    return is_file_missing


def _update_files(nodes, graph):
    for task_name, _ in nodes:
        task = graph[task_name]
        maid.monitor.time.touch_files(task.get_targets())
        maid.monitor.time.touch_files(task.get_required_files())
        maid.monitor.hash.make_hashes(task.get_targets())
        maid.monitor.hash.make_hashes(task.get_required_files())


def run(workreq):
    graph = workreq.main_tasks
    subgraph, root_nodes = [], []

    try:
        subgraph, root_nodes = maid.graph.get_subgraph(graph, workreq.task)
    except KeyError as e:
        msg = 'Required task `{req}` of task `{task}` not found for maid `{maid_name}`.'.format(req=str(e).strip("'"), task=workreq.task, maid_name=workreq.maid_name)
        raise TaskNotFoundException(msg)

    start_nodes = maid.graph.get_start_nodes(
            subgraph,
            root_nodes,
            _should_task_run(graph, use_hash=workreq.use_hash),
            )
    sorted_tasks = maid.graph.topo_sort(
            subgraph,
            (s[0] for s in start_nodes),
            )

    prev_sorted_tasks = sorted_tasks
    sorted_tasks = _add_startup_and_teardown_tasks(sorted_tasks, workreq)

    if not sorted_tasks:
        print('All tasks up to date.  Nothing to do.', file=sys.stderr)
        return
    if workreq.update_requested:
        _update_files(sorted_tasks, graph)
        return
    if workreq.dry_run:
        _print_dry_run(start_nodes, prev_sorted_tasks, workreq)
        return

    finished_tasks = set(s[1] for s in start_nodes)
    _run_tasks(sorted_tasks, graph, workreq, finished_tasks=finished_tasks)


def _print_dry_run(start_nodes, sorted_tasks, workreq):
    idx = 1

    print('Startup tasks:')
    for idx, task in enumerate(workreq.startup_tasks, 1):
        print('  {}) {}'.format(idx, task))

    cur_depth = -1
    j = 0
    for idx, (task, depth) in enumerate(sorted_tasks, idx + 1):
        if depth != cur_depth:
            cur_depth = depth
            print('Depth {}:'.format(depth))
        if depth == 0:
            update_reason = ''
            if task == start_nodes[j][1]:
                update_reason = 'missing target files'
            else:
                update_reason = 'required task `{}` has updated files'.format(start_nodes[j][1])
            print('  {}) {} ({})'.format(idx, task, update_reason))
        else:
            print('  {}) {}'.format(idx, task))
        j += 1

    print('Teardown tasks:')
    for idx, task in enumerate(workreq.teardown_tasks, idx + 1):
        print('  {}) {}'.format(idx, task))


def _add_startup_and_teardown_tasks(sorted_tasks, workreq):
    if not sorted_tasks:
        return sorted_tasks

    new_sorted_tasks = [(t, -1) for t in workreq.startup_tasks]

    for (t, d) in sorted_tasks:
        task = workreq.main_tasks[t]
        if task.dont_run_if_all_targets_exist:
            # TODO: what to do when no targets?
            if all(os.path.exists(f) for f in task.get_targets()):
                continue
        new_sorted_tasks.append((t, d))
    depth = sorted_tasks[-1][1] + 1

    new_sorted_tasks.extend([(t, depth) for t in workreq.teardown_tasks])

    sorted_tasks = tuple(new_sorted_tasks)
    return new_sorted_tasks


class MissingExpectedTargetsException(Exception):
    '''
    '''

    def __init__(self, msg):
        '''
        '''
        super().__init__(msg)


class TaskNotFoundException(Exception):
    '''
    '''

    def __init__(self, msg):
        '''
        '''
        super().__init__(msg)
