import json
import time
import requests
import pandas as pd

patch_date_release = 1588291200 # patch 7.26c
patch_date_end = 1593302400

teams = pd.read_pickle('data/pro_teams_three_runs.pkl')
matches = pd.read_pickle('data/pro_matches_list.pkl')

print('There are {} teams.'.format(len(teams)))

matches.drop_duplicates(keep='first', inplace=True)

print('Getting and parsing data for {} matches'.format(len(matches)))

used_columns = ['match_id', 'radiant_team_id', 'radiant_win', 'dire_team_id', 'radiant_score', 'dire_score', 'duration',
                'radiant_gold_adv', 'radiant_xp_adv', 'picks_bans']

pro_matches = pd.DataFrame([])
pro_matches_count = 0
err = 0

for match in matches:
    print('requesting match {}'.format(match), flush=True)
    r = requests.get('https://api.opendota.com/api/matches/{}'.format(match))
    pro_match = pd.read_json(r.text, lines=True, orient='columns')

    try:
        pro_match = pro_match[used_columns]
        pro_matches = pro_matches.append(pro_match)
        pro_matches_count = pro_matches_count + 1
    except:
        err = err + 1

    print(r)
    time.sleep(1)

del matches
print('Total professional matches are {0}. Errors are {1}.'.format(pro_matches_count, err))

# uncomment if raw matches data needs to be saved
# print('Saving data for {} matches.'.format(pro_matches_count))
# pro_matches.to_pickle('data\pro_matches_patch726c.pkl')

# uncomment to read existing raw matches data
# pro_matches = pd.read_pickle('data\pro_matches_patch726c.pkl')

print('Processing data...')

teams = pro_matches['radiant_team_id'].append(pro_matches['dire_team_id']).drop_duplicates(keep='first')
team_results = pd.DataFrame()

print('There are {} teams.'.format(len(teams)))

for team in teams:
    team_matches = pro_matches[(pro_matches['dire_team_id'] == team) | (pro_matches['radiant_team_id'] == team)]

    team_result = {}

    team_result['team'] = team
    team_result['matches'] = team_matches['match_id'].count()
    team_result['radiant_wins'] = team_matches[(team_matches['radiant_team_id'] == team) & team_matches['radiant_win']][
        'match_id'].count()
    team_result['dire_win'] = \
    team_matches[(team_matches['dire_team_id'] == team) & ~team_matches['radiant_win'].astype(bool)]['match_id'].count()
    team_result['radiant_losses'] = \
    team_matches[(team_matches['radiant_team_id'] == team) & ~team_matches['radiant_win'].astype(bool)][
        'match_id'].count()
    team_result['dire_losses'] = \
    team_matches[(team_matches['dire_team_id'] == team) & team_matches['radiant_win'].astype(bool)]['match_id'].count()
    team_result['kills'] = (team_matches[team_matches['radiant_team_id'] == team].eval(
        'radiant_score - dire_score').sum() + team_matches[team_matches['dire_team_id'] == team].eval(
        'dire_score - radiant_score').sum()) / len(team_matches)
    team_result['duration'] = team_matches['duration'].mean()
    team_result['gold_advantage'] = ((team_matches.loc[
                                          ~(team_matches['radiant_team_id'] == team), 'radiant_gold_adv'].str[
                                          -1] * -1).sum() +
                                     team_matches.loc[team_matches['radiant_team_id'] == team, 'radiant_gold_adv'].str[
                                         -1].sum()) / len(team_matches)
    team_result['xp_advantage'] = ((team_matches.loc[~(team_matches['radiant_team_id'] == team), 'radiant_xp_adv'].str[
                                        -1] * -1).sum() +
                                   team_matches.loc[team_matches['radiant_team_id'] == team, 'radiant_xp_adv'].str[
                                       -1].sum()) / len(team_matches)

    picks = pd.Series(dtype=int)
    bans = pd.Series(dtype=int)
    team_heroes = team_matches['picks_bans'].dropna()

    for heroes in team_heroes:
        hero = pd.DataFrame(heroes)
        hero = hero.groupby(by=['team', 'is_pick'])['hero_id'].apply(list).reset_index()

        if team_matches['radiant_team_id'].iloc[0] == team:
            picks = picks.append(hero.loc[(hero['team'] == 0) & hero['is_pick'], 'hero_id'])
            bans = bans.append(hero.loc[(hero['team'] == 1) & ~hero['is_pick'], 'hero_id'])
        else:
            picks = picks.append(hero.loc[(hero['team'] == 1) & hero['is_pick'], 'hero_id'])
            bans = bans.append(hero.loc[(hero['team'] == 0) & ~hero['is_pick'], 'hero_id'])

    del team_heroes
    team_result['unique_heroes_played'] = picks.explode().nunique()
    team_result['unique_heroes_banned_against'] = bans.explode().nunique()

    team_results = team_results.append(team_result, ignore_index=True)

team_results.to_pickle('data\pro_teams_results.pkl')