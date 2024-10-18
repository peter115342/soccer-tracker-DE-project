# src/main.py

from utils.api_helpers import fetch_all_fixtures_by_date, LEAGUE_IDS, filter_fixtures_by_leagues, fetch_match_statistics
from utils.data_processing import process_fixtures, process_match_statistics
from utils.bigquery_helpers import insert_data_into_bigquery
import datetime
import logging

def main(request):
    date = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    logging.info(f"Fetching fixtures for date: {date}")

    all_fixtures = fetch_all_fixtures_by_date(date)
    fixtures = filter_fixtures_by_leagues(all_fixtures, LEAGUE_IDS)

    if fixtures:
        logging.info(f"Found {len(fixtures)} fixtures for selected leagues.")
        fixtures_data = process_fixtures(fixtures)
        insert_data_into_bigquery('fixtures', fixtures_data)

        for index, fixture in enumerate(fixtures):
            fixture_id = fixture['fixture']['id']
            fixture_date = fixture['fixture']['date'][:10]
            stats_list = fetch_match_statistics(fixture_id, index, len(fixtures))
            if stats_list:
                stats_data = process_match_statistics(stats_list, fixture_date)
                insert_data_into_bigquery('match_statistics', stats_data)
            else:
                logging.info(f"No statistics available for fixture {fixture_id}.")
    else:
        logging.info("No fixtures found for selected leagues today.")
    return 'Data ingestion completed.'
