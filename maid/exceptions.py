

class InvalidTaskNameException(Exception):

    def __init__(self):
        '''
        '''
        msg = 'Task names must not be empty.'
        super().__init__(msg)


class DefaultTaskRunPhaseException(Exception):

    def __init__(self, task):
        '''
        '''
        msg = 'Only pipelines in the `NORMAL` run phase can be a default; was given `{}` for task `{}`.'.format(
            task.run_phase,
            task.name,
            )
        super().__init__(msg)


class MaidNameException(Exception):

    def __init__(self):
        '''
        '''
        msg = 'Maid name must not be empty.'
        super().__init__(msg)


class DuplicateTaskException(Exception):

    def __init__(self, task):
        '''
        '''
        msg = 'Maid `{}` already has task named `{}`.'.format(
                    task.maid_name,
                    task.name,
                    )
        super().__init__(msg)


class UnknownTaskException(Exception):

    def __init__(self, task):
        '''
        '''
        msg = 'Unknown task.  Maid `{}` has no task named `{}`'.format(
                    task.maid_name,
                    task.name,
                    )
        super().__init__(msg)


class UnknownCommandTypeException(Exception):

    def __init__(self, command):
        '''
        '''
        msg = 'Unknown command type used with `|`: {}.  Only `str`, `callable`, and `tuple` instances are supported.'.format(command)
        super().__init__(msg)


class MissingTargetException(Exception):
    '''
    '''

    def __init__(self, task, filename):
        '''
        '''
        msg = 'Task `{task}` ran without error but did not create expected files: `{filename}` not found.'.format(task=task.name, filename=filename)
        super().__init__(msg)
