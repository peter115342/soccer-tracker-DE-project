from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from utils.api_helpers import get_league_data
from utils.bigquery_helpers import insert_data_into_bigquery

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fetch_league_data_dag',
    default_args=DEFAULT_ARGS,
    description='Fetch leagues and teams data and load into BigQuery',
    schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    @task
    def fetch_and_process_data():
        league_codes = ['PL', 'FL1', 'BL1', 'SA', 'PD']  # Premier League, Ligue 1, Bundesliga, Serie A, La Liga
        league_data_list = []

        for code in league_codes:
            league_data = get_league_data(code)
            league_data_list.append(league_data)

        return league_data_list

    @task
    def load_data_into_bigquery(league_data_list):
        league_table_name = 'leagues'
        team_table_name = 'teams'

        leagues = []
        teams = []

        for league_data in league_data_list:
            league_info = {
                'id': league_data['id'],
                'name': league_data['name'],
                'code': league_data['code'],
                'logo': league_data.get('emblem'),
                'current_season_start_date': league_data['currentSeason']['startDate'],
                'current_season_end_date': league_data['currentSeason']['endDate'],
                'current_season_matchday': league_data['currentSeason'].get('currentMatchday'),
            }
            leagues.append(league_info)

            for team in league_data['teams']:
                team_info = {
                    'id': team['id'],
                    'name': team['name'],
                    'tla': team.get('tla'),
                    'logo': team.get('crest'),
                    'venue': team.get('venue'),
                    'league_id': league_data['id'],
                }
                teams.append(team_info)

        insert_data_into_bigquery(league_table_name, leagues)
        insert_data_into_bigquery(team_table_name, teams)

    data = fetch_and_process_data()
    load_data_into_bigquery(data)
