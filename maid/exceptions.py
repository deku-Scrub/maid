

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
        msg = 'Only pipelines in the `NORMAL` run phase can be a default; was given `{}` for pipeline `{}`.'.format(
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