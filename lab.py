import maid

main_maid = maid.get_maid()
main_maid.add_task(
        'xa',
        lambda i, o, t, mit: print('xa'),
        targets=[],
        required_tasks=[],
        required_files=[],
        run_at_start=True,
        run_at_end=True,
        )
main_maid.add_task(
        'x0',
        lambda rf, rt, t, mrt: f'touch {t[0]}',
        targets=['0'],
        required_tasks=[],
        required_files=[],
        )
main_maid.add_task(
        'x1',
        lambda rf, rt, t, mrt: f'touch {t[0]}',
        targets=['1'],
        required_tasks=[],
        required_files=[],
        #always_run_if_in_graph=True,
        )
main_maid.add_task(
        'x2',
        lambda rf, rt, t, mrt: f'touch {t[0]}',
        targets=['2'],
        required_tasks=['x1'],
        required_files=[],
        )
main_maid.add_task(
        'x3',
        lambda rf, rt, t, mrt: f'touch {t[0]}',
        targets=['3'],
        required_tasks=['x2'],
        required_files=[],
        )
main_maid.add_task(
        'x4',
        lambda rf, rt, t, mrt: f'touch {t[0]}',
        targets=['4'],
        required_tasks=['x2'],
        required_files=[],
        #dont_run_if_all_targets_exist=True,
        )
main_maid.add_task(
        'x5',
        lambda rf, rt, t, mrt: (print(mrt), f'touch {t[0]}')[1],
        targets=['5'],
        required_tasks=['x3', 'x4'],
        required_files=[],
        )
main_maid.add_task(
        'x6',
        lambda rf, rt, t, mrt: f'touch {t[0]}',
        targets=['6'],
        required_tasks=['x1', 'x5', 'x7'],
        required_files=[],
        is_default=True,
        )
main_maid.add_task(
        'x7',
        lambda rf, rt, t, mrt: f'touch {t[0]}; echo lalala',
        #lambda rf, rt, t, mrt: f'touch {t[0]}; lalala',
        #lambda rf, rt, t, mrt: 'lol'.replace('l', 'o') if False else False,
        #lambda rf, rt, t, mrt: print('lalal'),
        targets=['7'],
        required_tasks=['x0'],
        required_files=[],
        #delete_targets_on_error=False,
        print_script=False,
        print_script_output=True,
        #shell=('python3', '-c'),
        )

#main_maid.describe_goals()
#main_maid.describe_tasks()
#main_maid.work('x6', dry_run=True)
