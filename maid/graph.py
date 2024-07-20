# TODO:
#   * pass to recipe files that have changed (make's `$?`)
#   * independent targets (run script per target, not once with all targets)
#   * map_targets (how required file is mapped to target, (both have `*.ext`))
#   * not parallel (run task in parallel or no (put it in its own depth))
#   * vpath (directories in which to search for files)
import os
import logging

logger = logging.getLogger(__name__)
index_dir = os.path.join('.maid', 'index')
os.makedirs(index_dir, exist_ok=True)

def get_subgraph(graph, root_node):
    visited = set()
    subgraph = {root_node: set()}
    q = [root_node]
    root_nodes = []
    while q:
        node = q.pop(0)
        if not graph[node].required_tasks:
            root_nodes.append(node)
        visited.add(node)
        q.extend(c for c in graph[node].required_tasks if c not in visited)
        for c in graph[node].required_tasks:
            subgraph.setdefault(c, set()).add(node)

    logger.debug('roots: %s', root_nodes)
    logger.debug('subgraph: %s', subgraph)

    return subgraph, root_nodes


def get_start_nodes(graph, root_nodes, is_start_node):
    visited = set()
    start_nodes = []
    q = list(root_nodes)
    while q:
        node = q.pop(0)
        visited.add(node)
        if (s := is_start_node(node)):
            start_nodes.append((node, s[0], s[1]))
            continue
        q.extend(c for c in graph[node] if c not in visited)

    logger.debug('starts: %s', start_nodes)

    return start_nodes


def topo_sort(graph, root_nodes):
    visited = set()
    q = list((n, 0) for n in root_nodes)
    node_to_idx = {n: d for n, d in q}
    while q:
        node, depth = q.pop(0)
        visited.add(node)
        child_depth = depth + 1
        for child in graph[node]:
            prev_idx = node_to_idx.setdefault(child, 0)
            node_to_idx[child] = max(prev_idx, child_depth)
            if child in visited:
                continue
            q.append((child, child_depth))

    sorted_tasks = sorted((i for i in node_to_idx.items()), key=lambda i: i[1])

    logger.debug('sorted: %s', sorted_tasks)

    return sorted_tasks
