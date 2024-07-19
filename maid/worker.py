import sys

import maid.tasks
import maid.task_runner


_maids = dict()


def get_maid(name=''):
    '''
    '''
    name = name if name else 'Default maid'
    if not isinstance(name, str):
        raise Exception()
    return _maids.setdefault(name, _Maid(name))


def exists(name):
    '''
    '''
    return name in _maids


def describe_maids(names=None):
    names = names if names else _maids.keys()
    for name in names:
        if name not in _maids:
            print('Maid `{}` not found.'.format(name))
        else:
            print('{}'.format(name))


def describe_tasks(maid_names=None):
    maid_names = maid_names if maid_names else _maids.keys()
    for name in maid_names:
        print()
        if name not in _maids:
            print('Maid `{}` not found.'.format(name))
            continue
        else:
            print('Maid name: {}'.format(name))

        print()
        workflows = []
        if (w := _maids[name].startup_tasks):
            workflows.append(('Startup tasks', w))
        if (w := _maids[name].teardown_tasks):
            workflows.append(('Teardown tasks', w))
        if (w := _maids[name].main_tasks):
            workflows.append(('Main tasks', w))
        for (title, workflow) in workflows:
            print('  {}:'.format(title))
            for (task_name, task) in workflow.items():
                print('    {}'.format(task_name))
                print('      {}'.format(task.description))
            print()


class _Maid:
    '''
    '''

    def __init__(self, name):
        '''
        '''
        self.name = name
        self.main_tasks = dict()
        self.startup_tasks = dict()
        self.teardown_tasks = dict()
        self.default_task = ''

    def add_task(
        self,
        name,
        recipe,
        *,
        targets=None,
        required_tasks=None,
        required_files=None,
        targets_regex_mappings=None,
        description='Please add a task description.',
        delete_targets_on_error=True,
        print_script=sys.stdout,
        print_script_output=sys.stdout,
        always_run_if_in_graph=False,
        dont_run_if_all_targets_exist=False,
        run_at_start=False,
        run_at_end=False,
        is_default=False,
        shell=None,
        ):
        if is_default:
            if self.default_task:
                raise Exception()
            self.default_task = name

        if run_at_start and required_tasks:
            raise Exception()
        if run_at_end and required_tasks:
            raise Exception()

        cur_workflows = [self.main_tasks]
        if run_at_start:
            cur_workflows.append(self.startup_tasks)
        if run_at_end:
            cur_workflows.append(self.teardown_tasks)

        #if targets_regex_mappings:
            #targets_regex_mappings = (('d/*', 'a(.*)b', 'x\1y'),)
            #for file_glob, regex, target_sub in targets_regex_mappings:
                #pattern = re.compile(regex)
                #for filename in maid.tasks._get_filenames(file_glob):
                    #if (r := pattern.subn(target_sub, filename)) and r[1] > 0:
                        #target = r[0]

        for workflow in cur_workflows:
            if name in workflow:
                raise Exception()

            workflow[name] = maid.tasks.Task(
                    name=name,
                    recipe=recipe,
                    targets=targets,
                    required_tasks=required_tasks,
                    required_files=required_files,
                    description=description,
                    delete_targets_on_error=delete_targets_on_error,
                    print_script=print_script,
                    print_script_output=print_script_output,
                    always_run_if_in_graph=always_run_if_in_graph,
                    dont_run_if_all_targets_exist=dont_run_if_all_targets_exist,
                    shell=shell,
                )

    def work(
            self,
            task='',
            *,
            dry_run=False,
            use_hash=False,
            update_requested=False,
            finish_depth_on_failure=False,
            ):
        '''
        '''
        if (not task) and (not self.default_task):
            msg = 'Maid has no default task.  Specify a task in the command line or use `is_default` when defining a task to designate it as the default for maid `{name}`.'.format(name=self.name)
            raise NoDefaultTaskException(msg)

        workreq = maid.tasks.WorkRequest(
                maid_name=self.name,
                task=task if task else self.default_task,
                main_tasks=self.main_tasks,
                teardown_tasks=self.teardown_tasks,
                startup_tasks=self.startup_tasks,
                dry_run=dry_run,
                use_hash=use_hash,
                update_requested=update_requested,
                finish_depth_on_failure=finish_depth_on_failure,
                )

        maid.task_runner.run(workreq)


class NoDefaultTaskException(Exception):
    '''
    '''

    def __init__(self, msg):
        '''
        '''
        super().__init__(msg)
