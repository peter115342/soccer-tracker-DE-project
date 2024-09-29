import os
from utils.api_helpers import fetch_fixtures, fetch_match_statistics
from utils.bigquery_helpers import insert_data_into_bigquery

def main(request):
    """
    Entry point for the Cloud Function.
    """
    fixtures = fetch_fixtures()
    for fixture in fixtures:
        match_stats = fetch_match_statistics(fixture['fixture_id'])
        insert_data_into_bigquery(match_stats)
    return 'Data successfully fetched and stored.'
