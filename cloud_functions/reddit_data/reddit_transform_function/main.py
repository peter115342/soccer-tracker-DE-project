import base64
import json
import os
import logging
import time
from google.cloud import bigquery, dataform_v1beta1
from datetime import datetime
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


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

    compile_response = client.create_compilation_result(
        parent=repository,
        compilation_result={"git_commitish": "main"},
    )

    workflow_invocation = {
        "compilation_result": compile_response.name,
        "invocation_config": {"included_tags": ["reddit"]},
    }

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


def transform_reddit(event, context):
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "transform_reddit":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Reddit Transform: Invalid Trigger", error_message, 16711680
            )
            return error_message, 400

        workflow_result = trigger_dataform_workflow()
        logging.info(f"Workflow result: {workflow_result}")

        workflow_state = workflow_result.state
        status_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        client = bigquery.Client()
        count_query = (
            "SELECT COUNT(*) as reddit_count FROM sports_data_eu.reddit_processed"
        )
        count_job = client.query(count_query)
        reddit_count = next(count_job.result())[0]

        status_message = (
            f"Reddit transformation completed successfully.\n"
            f"Total reddit records processed: {reddit_count}\n"
            f"DataForm workflow state: {workflow_state}\n"
            f"Completion time: {status_time}"
        )

        logging.info(status_message)
        send_discord_notification("✅ Reddit Transform: Success", status_message, 65280)

        return status_message, 200

    except Exception as e:
        error_message = f"Error transforming reddit data: {str(e)}"
        logging.error(error_message)
        send_discord_notification(
            "❌ Reddit Transform: Failure", error_message, 16711680
        )
        return error_message, 500
