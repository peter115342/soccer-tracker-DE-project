import json
import os
import logging
from datetime import datetime
from google.cloud import firestore, storage
import base64
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def sync_summaries_to_firestore(event, context):
    """Cloud Function to sync match summaries from GCS to Firestore."""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "sync_summaries_to_firestore":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Summaries Firestore Sync: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        db = firestore.Client()
        storage_client = storage.Client()
        bucket_name = os.environ.get("BUCKET_NAME")
        if not bucket_name:
            raise ValueError("BUCKET_NAME environment variable not set")

        bucket = storage_client.bucket(bucket_name)
        summaries_collection = db.collection("match_summaries")

        sync_count = 0
        blobs = bucket.list_blobs(prefix="match_summaries/")

        for blob in blobs:
            if not blob.name.endswith(".md"):
                continue

            doc_id = blob.name.split("/")[-1].replace(".md", "")

            doc_ref = summaries_collection.document(doc_id)
            doc = doc_ref.get()

            if doc.exists:
                logging.info(
                    f"Document {doc_id} already exists in Firestore. Skipping."
                )
                continue

            content = blob.download_as_text()
            firestore_data = {
                "content": content,
                "filename": blob.name,
                "created_at": datetime.now().isoformat(),
                "last_updated": datetime.now().isoformat(),
            }

            doc_ref.set(firestore_data, merge=False)
            sync_count += 1

        status_message = (
            f"Successfully synced {sync_count} match summaries to Firestore"
        )
        logging.info(status_message)
        send_discord_notification(
            "✅ Summaries Firestore Sync: Success", status_message, 65280
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Summaries Firestore sync: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Summaries Firestore Sync: Failure", error_message, 16711680
        )
        return error_message, 500
