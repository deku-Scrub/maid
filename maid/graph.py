import os
import subprocess


def _do_all_files_exists(filenames):
    for filename in filenames:
        if not os.path.exists(filename):
            return False, filename
    return True, ''


def _is_newer(filename, other_time):
    if not os.path.exists(filename):
        return True
    return os.path.getmtime(filename) > other_time


def _is_up_to_date(target, reqs, graph):
    if not os.path.exists(target):
        return False

    target_time = os.path.getmtime(target)
    for req in reqs:
        cur_reqs = graph[req]['outputs'] if req in graph else (req,)
        if any(_is_newer(r, target_time) for r in cur_reqs):
            return False
    return True


class Graph:
    '''
    '''

    def __init__(self):
        '''
        '''
        self._graph = dict()

    def update(self, graph):
        '''
        '''
        for k, v in graph.items():
            if k in self._graph:
                raise Exception('Error when updating graph: task name already exists: {}'.format(k))
            if 'inputs' not in v:
                raise Exception('Error when updating graph: task `{}` is missing the `inputs` field.  If it has no inputs, set `inputs` to an empty list.'.format(k))
            if 'outputs' not in v:
                raise Exception('Error when updating graph: task `{}` is missing the `outputs` field.  If it has no outputs, set `outputs` to an empty list.'.format(k))
            if 'function' not in v:
                raise Exception('Error when updating graph: task `{}` is missing the `function` field.  If it has no outputs, set `outputs` to an empty list.'.format(k))
            if type(v['inputs']) != tuple:
                raise Exception('Error when updating graph: value of task `{}`\'s `inputs` field is not a tuple.'.format(k))
            if type(v['outputs']) != tuple:
                raise Exception('Error when updating graph: value of task `{}`\'s `outputs` field is not a tuple.'.format(k))
            if type(v['function']) != type(lambda x: 1):
                raise Exception('Error when updating graph: value of task `{}`\'s `function` field is not a function.'.format(k))
            self._graph[k] = v

    def dry_run(self, task):
        '''
        '''
        self._run(task, dry=True)

    def run(self, task):
        '''
        '''
        self._run(task, dry=False)

    def _is_root_node(inputs, graph):
        return all(i not in graph for i in inputs)

    def _validate_graph(graph):
        for k, v in graph.items():
            if Graph._is_root_node(v['inputs'], graph):
                if (ret := _do_all_files_exists(v['inputs'])) and ret[0]:
                    continue
                # TODO: make new exception class with this message
                msg = 'Root task `{}` is missing required file `{}`.  Root tasks depend on either nothing or on files that already exist.  You might have forgotten to generate the missing file, or the task is not a root and you forgot to add to `inputs` the task that generates the file.'.format(k, ret[1])
                raise Exception(msg)

    def _run(self, task, dry=True):
        '''
        '''
        if task not in self._graph:
            raise Exception('Error running graph: task name never defined: {}'.format(task))
        Graph._validate_graph(self._graph)

        subgraph, root_nodes = Graph._get_subgraph(task, self._graph)
        nodes = Graph._topological_sort(subgraph, root_nodes)

        for node, _ in nodes:
            inputs = self._graph[node]['inputs']
            outputs = self._graph[node]['outputs']

            if all(_is_up_to_date(o, inputs, self._graph) for o in outputs):
                continue

            if dry:
                print('will run task `{}`'.format(node))
                continue

            script = self._graph[node]['function'](inputs, outputs)
            if not self._run_script(script):
                # TODO: remove outputs?
                print('Error running task `{}`.'.format(node))
                return

    def _run_script(self, script):
        print('{}'.format(script))
        results = None
        results = subprocess.run(['bash', '-c', script], capture_output=True)

        if results.returncode == 0:
            return True

        print(results.stderr.decode('utf-8'))
        return False

    def _get_subgraph(root, graph):
        '''
        '''
        queued_nodes = [root]
        visited = set()
        root_nodes = []
        subgraph = dict()
        while queued_nodes:
            node = queued_nodes.pop(0)

            if node in visited:
                # TODO: detect loops.
                continue

            visited.add(node)
            subgraph.setdefault(node, [])

            is_root_node = True
            for child in graph[node]['inputs'][::-1]:
                if child in graph:
                    queued_nodes.append(child)
                    subgraph.setdefault(child, []).append(node)
                    is_root_node = False

            if is_root_node:
                root_nodes.append(node)

        return subgraph, root_nodes

    def _topological_sort(graph, root_nodes):
        '''
        '''
        node_to_idx = {node: 0 for node in graph}
        for root_node in root_nodes:
            queued_nodes = [(root_node, 0)]
            visited = set()
            while queued_nodes:
                node, depth = queued_nodes.pop(0)

                if node in visited:
                    continue

                visited.add(node)

                for child in graph[node]:
                    queued_nodes.append((child, depth + 1))
                    node_to_idx[child] = max(node_to_idx[child], depth + 1)

        sorted_nodes = sorted(node_to_idx.items(), key=lambda p: p[1])
        return sorted_nodes
