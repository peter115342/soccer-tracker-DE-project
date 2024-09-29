source .env

gcloud functions deploy fetch_sports_data \
    --runtime python39 \
    --trigger-http \
    --entry-point main \
    --set-env-vars API_FOOTBALL_KEY=$API_FOOTBALL_KEY \
    --project $GCP_PROJECT_ID