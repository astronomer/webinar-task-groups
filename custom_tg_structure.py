from airflow.utils.task_group import TaskGroup


class MyCustomTaskGroup(TaskGroup):
    def __init__(self, group_id, ui_color="#00A7FB", **kwargs): 
        super().__init__(
            group_id=group_id, ui_color=ui_color, **kwargs
        )

        # add tasks here


