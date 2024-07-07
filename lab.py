import maid.graph


def get_deps(inputs, outputs):
    '''
    '''
    return f'python3 -m pip install -t deps -r {inputs[0]} && touch deps'


task_graph = {
    'mkdir': {
        'inputs': ('maid',),
        'outputs': ('mkdir',),
        'function': lambda i, o: f'touch {o[0]}',
    },
    'get_deps': {
        'inputs': ('requirements.txt',),
        'outputs': ('deps',),
        'function': get_deps,
    },
    'download': {
        'inputs': ('get_deps',),
        'outputs': ('download',),
        'function': lambda i, o: f'touch {o[0]}',
    },
    'size': {
        'inputs': ('download',),
        'outputs': ('size',),
        'function': lambda i, o: f'touch {o[0]}',
    },
    'count': {
        'inputs': ('download', 'mkdir'),
        'outputs': ('count',),
        'function': lambda i, o: f'touch {o[0]}',
    },
    'find': {
        'inputs': ('size',),
        'outputs': ('find',),
        'function': lambda i, o: f'touch {o[0]}',
    },
    'synonyms': {
        'inputs': ('size', 'count'),
        'outputs': ('synonyms',),
        'function': lambda i, o: f'touch {o[0]}',
    },
    'db': {
        'inputs': ('synonyms', 'download'),
        'outputs': ('db',),
        'function': lambda i, o: f'touch {o[0]}',
    },
}

graph = maid.graph.Graph()
graph.update(task_graph)
graph.dry_run('help')
#graph.dry_run('db')
#graph.run('db')
