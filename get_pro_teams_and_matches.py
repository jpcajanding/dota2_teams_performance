import json
import time
import requests
import pandas as pd

patch_date_release = 1588291200 # patch 7.26c
patch_date_end = 1593302400

teams_not_collected = pd.Series([], dtype=int)
teams_not_collected_second = pd.Series([], dtype=int)
matches = pd.Series([], dtype=int)

# get initial list of teams
teams = requests.get('https://api.opendota.com/api/teams').json()
teams = pd.DataFrame.from_dict(teams)
teams = teams[teams['last_match_time'] >= patch_date_release] # get active teams for this patch 7.26c
teams = teams['team_id']

print(len(teams))


def request_team_matches_for_period(team, patch_date_release, patch_date_end):
    url = 'https://api.opendota.com/api/teams/' + str(team) + '/matches'
    print('Getting matches for team {}'.format(team), flush=True)
    response = requests.get(url)
    print(response)
    team_matches = pd.DataFrame.from_dict(response.json())
    team_matches = team_matches[(team_matches['start_time'] >= patch_date_release) & (team_matches['start_time'] < patch_date_end)]
    return team_matches


for team in teams:
    team_matches = request_team_matches_for_period(team, patch_date_release, patch_date_end)
    if len(team_matches) >= 5:
        unsollicited_teams = team_matches[~team_matches['opposing_team_id'].isin(teams)]
        teams_not_collected = teams_not_collected.append(unsollicited_teams['opposing_team_id'])
        matches = matches.append(team_matches['match_id'], ignore_index=True)
    time.sleep(1)

print('Done!')

teams_not_collected = teams_not_collected.drop_duplicates(keep='first')
teams = teams.append(teams_not_collected)
teams = teams.drop_duplicates(keep='first')

print('Teams not collected: {}'.format(len(teams_not_collected)))
print('Getting second run of teams...')

for team in teams_not_collected:
    team_matches = request_team_matches_for_period(team, patch_date_release, patch_date_end)
    if len(team_matches) >= 5:
        unsollicited_teams = team_matches[~team_matches['opposing_team_id'].isin(teams)]
        teams_not_collected_second = teams_not_collected_second.append(unsollicited_teams['opposing_team_id'])
        matches = matches.append(team_matches['match_id'], ignore_index=True)
    time.sleep(1)

teams_not_collected_second = teams_not_collected_second.drop_duplicates(keep='first')
teams = teams.append(teams_not_collected_second)
teams = teams.drop_duplicates(keep='first')

print('Teams not collected: {}'.format(len(teams_not_collected_second)))
print('Getting last run of teams...')

for team in teams_not_collected_second:
    team_matches = request_team_matches_for_period(team, patch_date_release, patch_date_end)
    if len(team_matches) >= 5:
        matches = matches.append(team_matches['match_id'], ignore_index=True)
    time.sleep(1)

print('Total teams collected: {}'.format(len(teams)))
print('Total matches: {}'.format(len(matches)))

teams.to_pickle('data\pro_teams_three_runs.pkl')
matches.to_pickle('data\pro_matches_list.pkl')

