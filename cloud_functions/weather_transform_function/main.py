import base64
import json
import os
import logging
import time
import requests
from google.cloud import bigquery, pubsub_v1, dataform_v1beta1
from datetime import datetime


def trigger_dataform_workflow():
    client = dataform_v1beta1.DataformClient()

    project_id = os.environ["GCP_PROJECT_ID"]
    location = "europe-central2"
    repository_id = os.environ["DATAFORM_REPOSITORY"]

    repository = client.repository_path(
        project=project_id,
        location=location,
        repository=repository_id,
    )

    compilation_result = dataform_v1beta1.CompilationResult({"git_commitish": "main"})

    compile_response = client.create_compilation_result(
        parent=repository,
        compilation_result=compilation_result,
    )

    workflow_invocation = dataform_v1beta1.WorkflowInvocation(
        {"compilation_result": compile_response.name}
    )

    invocation_response = client.create_workflow_invocation(
        parent=repository,
        workflow_invocation=workflow_invocation,
    )

    workflow_invocation_name = invocation_response.name

    while True:
        current_workflow_invocation = client.get_workflow_invocation(
            name=workflow_invocation_name
        )
        state = current_workflow_invocation.state

        if state in [
            dataform_v1beta1.WorkflowInvocation.State.SUCCEEDED,
            dataform_v1beta1.WorkflowInvocation.State.FAILED,
            dataform_v1beta1.WorkflowInvocation.State.CANCELLED,
        ]:
            break

        time.sleep(5)

    return current_workflow_invocation


def send_discord_notification(title: str, message: str, color: int):
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.error("Discord webhook URL not configured")
        return

    if len(message) > 2000:
        message = message[:1997] + "..."

    discord_data = {
        "content": None,
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": color,
            }
        ],
    }

    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            webhook_url, data=json.dumps(discord_data), headers=headers, timeout=10
        )
        response.raise_for_status()
        if response.status_code != 204:
            logging.error(
                f"Discord notification failed with status code: {response.status_code}"
            )
    except requests.exceptions.RequestException as e:
        logging.error(f"Discord notification failed: {str(e)}")


def transform_weather(event, context):
    """Cloud Function to transform weather data using DataForm"""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "transform_weather":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Weather Transform: Invalid Trigger", error_message, 16711680
            )
            return error_message, 400

        workflow_result = trigger_dataform_workflow()
        logging.info(f"Workflow result: {workflow_result}")

        workflow_state = workflow_result.state
        status_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        client = bigquery.Client()
        count_query = (
            "SELECT COUNT(*) as weather_count FROM sports_data_eu.weather_processed"
        )
        count_job = client.query(count_query)
        weather_count = next(count_job.result())[0]

        status_message = (
            f"Weather transformation completed successfully.\n"
            f"Total weather records processed: {weather_count}\n"
            f"DataForm workflow state: {workflow_state}\n"
            f"Completion time: {status_time}"
        )

        logging.info(status_message)
        send_discord_notification(
            "✅ Weather Transform: Success", status_message, 65280
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error transforming weather data: {str(e)}"
        logging.error(error_message)
        send_discord_notification(
            "❌ Weather Transform: Failure", error_message, 16711680
        )
        return error_message, 500
