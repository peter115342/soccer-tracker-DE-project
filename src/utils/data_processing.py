
def process_fixtures(fixtures):
    data_to_insert = []
    for fixture in fixtures:
        fixture_data = {
            "fixture_id": fixture['fixture']['id'],
            "league_id": fixture['league']['id'],
            "league_name": fixture['league']['name'],
            "season": fixture['league']['season'],
            "date": fixture['fixture']['date'][:10],  #(YYYY-MM-DD)
            "time": fixture['fixture']['date'][11:19],  #(HH:MM:SS)
            "timestamp": fixture['fixture']['timestamp'],
            "home_team_id": fixture['teams']['home']['id'],
            "home_team_name": fixture['teams']['home']['name'],
            "away_team_id": fixture['teams']['away']['id'],
            "away_team_name": fixture['teams']['away']['name'],
            "venue_id": fixture['fixture']['venue']['id'],
            "venue_name": fixture['fixture']['venue']['name'],
            "status": fixture['fixture']['status']['long'],
            "goals_home": fixture['goals']['home'],
            "goals_away": fixture['goals']['away'],
            "referee": fixture['fixture']['referee'],
        }
        data_to_insert.append(fixture_data)
    return data_to_insert

def process_match_statistics(stats_list, fixture_date):
    data_to_insert = []
    for team_stats in stats_list:
        stats_data = {
            "fixture_id": team_stats['fixture']['id'],
            "team_id": team_stats['team']['id'],
            "team_name": team_stats['team']['name'],
            "date": fixture_date,
        }
        for stat in team_stats['statistics']:
            stat_name = stat['type'].lower().replace(' ', '_').replace('%', 'percentage')
            stat_value = stat['value']
            if isinstance(stat_value, str) and stat_value.endswith('%'):
                stat_value = stat_value.strip('%')
            stats_data[stat_name] = stat_value
        data_to_insert.append(stats_data)
    return data_to_insert
