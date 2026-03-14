from airflow.models import Variable
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.context import Context

def task_fail_slack_alert(context: Context):
    """
    Sends a custom Slack message on task failure.
    """
    slack_token = Variable.get("SLACK_API_TOKEN")
    slack_channel = Variable.get("SLACK_ALERTS_CHANNEL", default_var="#data-pipeline-alerts")

    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url

    slack_msg = f"""
            :red_circle: Task Failed.
            *Dag*: {dag_id}
            *Task*: {task_id}
            *Execution Date*: {execution_date}
            *Log Url*: {log_url}
            """

    alert = SlackAPIPostOperator(
        task_id='slack_failed_alert',
        channel=slack_channel,
        token=slack_token,
        text=slack_msg
    )

    return alert.execute(context=context)
