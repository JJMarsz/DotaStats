import urllib.request
import requests
import sys
import sqlite3
import json

# Control flags
fail_error = 0
log_lvl = 2 #0-nothing, 1-ERROR,2-INFO,3-DEBUG
exec_phase = [4] #ALL, 1, 2, 3, 4, 5
cache_data = 1
db_file = 'stats.db'

# Some useful constants
start_2018 = 1513728000
od = 'https://api.opendota.com/api/'
hdr = { 'User-Agent' : 'im a robot beepboop' }
roles = {'1' : 'Core', '2' : 'Support', '4' : 'Mid'}
points = {'kills' : 0.3, 'deaths' : -0.3, 'lh_and_d' : 0.003, 'gpm' : 0.002, 'tower_kills' : 1, 'roshan_kills' : 1, 'teamfight' : 3, \
			'obs_placed' : 0.5, 'camps_stacked' : 0.5, 'rune_pickups' : 0.25, 'first_blood' : 4, 'stuns' : 0.05}# Deaths +3

#
# Subroutines
#

# error log
def error(msg):
    if log_lvl > 0: print('[ERROR] :: ' + msg)
    if fail_error:
        sys.exit()

# info log
def info(msg):
    if log_lvl > 1: print('[INFO] :: ' + msg)

# debug log
def debug(msg):
    if log_lvl > 2: print('[DEBUG] :: ' + msg)

# appends the key to the string
def apiCall(url, key):
    return json.loads(requests.get(od + url + '?api_key=' + key, headers=hdr).text)

# returns upper bound on number of matches
def getMatchLinks(team_id, num_tourneys, key):
    match_json = apiCall('teams/' + team_id + '/matches', key)
    match_links = []
    t_count = 0
    last_t = ''
    for i in range(int(num_tourneys)*20):
        if i >= len(match_json):
            break
        if match_json[i]['league_name'] != last_t:
            last_t = match_json[i]['league_name']
            t_count += 1
            if t_count > int(num_tourneys):
                break
        match_links.append(match_json[i]['match_id'])
    return match_links

# parse input params
# Format:
# key-string
# team_id1 num_tournaments
# team_id2 num_tournaments
# ...
# team_idN num_tournaments
# (MUST HAVE NEW LINE AT END)
def parseParams(params):
    dic = {'key' : params[:params.find('\n')]}
    params = params[params.find('\n')+1:]
    while params.find('\n') != -1:
        dic[params[:params.find(' ')]] = params[params.find(' ')+1:params.find('\n')]
        params = params[params.find('\n')+1:]
    return dic
    
# Given a list of player dictionaries, returns dict with given id
def getPlayer(l, acc_id):
    for p in l:
        if p['account_id'] == acc_id:
            return p

# extracts a column from list of lists
def extractColumn(q, i=0):
    data = []
    for d in q:
        data.append(d[i])
    return data

#    
# Main
#

# Validate args
if(len(sys.argv) > 2):
    error("Too many arguments :: Format is python scrapper (url to team's game series) (number of series)")
    sys.exit()
if(len(sys.argv) < 1):
    error("Too little arguments :: Format is python scrapper (url to team's game series) (number of series)")
    sys.exit()
param_file = open(sys.argv[1], 'r')
params = parseParams(param_file.read());
param_file.close()
key = params['key']
params.pop('key')
# Connect to the Database
info('Connecting to the DB...')
conn = sqlite3.connect(db_file)
cur = conn.cursor()

if 1 in exec_phase:
    info('Phase 1 - Scraping match and player data from OpenDota')
    # loop on each team
    for k in params.keys():
        info('Scraping team ' + k + '\'s matches')
        match_ids = getMatchLinks(k, params[k], key)
        info('Discovered ' + str(len(match_ids)) + ' matches in last ' + params[k] + ' tournaments...')
        r = 0
        for match_id in match_ids:
            cur.execute('SELECT * FROM match_data WHERE match_id = ?', [match_id,])
            if len(cur.fetchall()) == 0:
                match_data = apiCall('matches/' + str(match_id), key)
                player_dp = {}

                # gather match stats
                match_dp = [int(match_data['match_id']), int(match_data['series_id']), int(match_data['radiant_win']), int(match_data['dire_team_id']), \
                            int(match_data['radiant_team_id']), int(match_data['duration']), match_data['league']['name'], int(match_data['start_time'])-start_2018, None]
                debug(str(match_dp))
                cur.execute('INSERT INTO match_data VALUES (?,?,?,?,?,?,?,?,?)', match_dp)
                # gather player stats
                for player in match_data['players']:
                    debug(str(player))
                    player_dp = [int(player['account_id']), int(match_data['match_id']), int(player['hero_id']), int(player['isRadiant']), int(player['kills']), int(player['deaths']), 
                                int(player['last_hits']) + int(player['denies']), int(player['gold_per_min']), int(player['tower_kills']), \
                                int(player['roshan_kills']), float(player['teamfight_participation']), int(player['obs_placed']), \
                                int(player['camps_stacked']), int(player['rune_pickups']), int(player['firstblood_claimed']), float(player['stuns'])]
                    debug(str(player_dp))
                    cur.execute('INSERT INTO player_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', player_dp)
                debug('Committing data points for match ' + str(match_data['match_id']))
            else:
                r += 1
        conn.commit()
        info(str(r) + ' were redundant matches')

if 2 in exec_phase:
    info('Phase 2 - Generating lookups')
    cur.execute('SELECT team_id from team_lookup')
    teams = extractColumn(cur.fetchall())
    # verify team_lookup is filled
    info('Generating team_lookup')
    for k in params.keys():
        if int(k) in teams:
            info('Already have team ' + k)
        else:
            team = apiCall('teams/' + k, key)
            cur.execute('INSERT INTO team_lookup VALUES (?,?,?)',[k,team['name'].strip(),team['tag'].strip(),])
            info('Found team ' + k)
    conn.commit()

    # verify player_lookup is filled
    info('Generating player_lookup')
    cur.execute('SELECT MAX(start_time), tl.team_id, md.match_id FROM match_data AS md, team_lookup AS tl WHERE md.dire_team_id = tl.team_id GROUP BY tl.team_id')
    dire_data = cur.fetchall()
    cur.execute('SELECT MAX(start_time), tl.team_id, md.match_id FROM match_data AS md, team_lookup AS tl WHERE md.radiant_team_id = tl.team_id GROUP BY tl.team_id')
    radiant_data = cur.fetchall()
    pros = apiCall('/proPlayers', key)
    recent_matches = []
    for i in range(len(radiant_data)):
        if radiant_data[i][0] > dire_data[i][0]:
            recent_matches.append([radiant_data[i][1],radiant_data[i][2], 1])
        else:
            recent_matches.append([dire_data[i][1],dire_data[i][2], 0])
    for match in recent_matches:
        cur.execute('SELECT account_id FROM player_lookup WHERE team_id = ?', [match[0],])
        num_players = len(cur.fetchall())
        if num_players == 0:
            info('Gathering players for team ' + str(match[0]))
            players = apiCall('/matches/' + str(match[1]), key)['players']
            for player in players:
                if match[2] == player['isRadiant']:
                    role = getPlayer(pros, player['account_id'])['fantasy_role']
                    cur.execute('INSERT INTO player_lookup VALUES (?,?,?,?)', [int(player['account_id']), player['name'], int(match[0]), role])
        elif num_players == 5:
        	info('Already have players for team ' + str(match[0]))
        elif num_players > 5:
            error('Team ' + str(match[1]) + ' has too many players (' + str(num_players) + ')')
        else:
            error('Team ' + str(match[1]) + ' isn\'t empty and has too little players (' + str(num_players) + ')')
    conn.commit()

    info('Generating hero_lookup')
    heroes = apiCall('/heroes', key)
    cur.execute('SELECT hero_id FROM hero_lookup')
    loaded_heroes = extractColumn(cur.fetchall())
    for hero in heroes:
        if hero['id'] not in loaded_heroes:
            cur.execute('INSERT INTO hero_lookup VALUES (?,?,?)',[hero['id'],hero['localized_name'],hero['legs'],])
            info('Inserted ' + hero['localized_name'])

if 3 in exec_phase:
    info('Phase 3 - Appending info to data tables')
    conn.commit()

if 4 in exec_phase:
    info('Phase 4 - Aggregating data into summary tables in DB')
    cur.execute('SELECT * FROM player_summary')
    if len(cur.fetchall()) > 0:
    	info('Flushing player summary table...')
    	cur.execute('DELETE FROM player_summary')
    	conn.commit()
    info('Populating player_summary table...')
    cur.execute('SELECT * FROM player_lookup')
    players = cur.fetchall()
    for player in players:
    	cur.execute('SELECT AVG(kills), AVG(deaths), AVG(lh_and_d), AVG(gpm), AVG(tower_kills), AVG(roshan_kills), AVG(teamfight), \
    		AVG(obs_placed), AVG(camps_stacked), AVG(rune_pickups), AVG(first_blood), AVG(stuns) FROM player_data WHERE account_id = ?', [player[0],])
    	stats = cur.fetchall()
    	for stat_pt in stats:
	    	stat_dp = stat_pt[0]*points['kills'] + stat_pt[1]*points['deaths'] + 3 + stat_pt[2]*points['lh_and_d'] + stat_pt[3]*points['gpm'] + \
	    			stat_pt[4]*points['tower_kills'] + stat_pt[5]*points['roshan_kills'] + stat_pt[6]*points['teamfight'] + stat_pt[7]*points['obs_placed'] + \
	    			stat_pt[8]*points['camps_stacked'] + stat_pt[9]*points['rune_pickups'] + stat_pt[10]*points['first_blood'] + stat_pt[11]*points['stuns']
	    	cur.execute('INSERT INTO player_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', \
	    															[player[1], roles[player[3]], stat_dp, stat_pt[0]*points['kills'], stat_pt[1]*points['deaths'] + 3, \
	    															stat_pt[2]*points['lh_and_d'], stat_pt[3]*points['gpm'], stat_pt[4]*points['tower_kills'], \
	    															stat_pt[5]*points['roshan_kills'], stat_pt[6]*points['teamfight'], stat_pt[7]*points['obs_placed'], \
	    															stat_pt[8]*points['camps_stacked'], stat_pt[9]*points['rune_pickups'], stat_pt[10]*points['first_blood'], \
	    															stat_pt[11]*points['stuns']])
    conn.commit()

if 5 in exec_phase:
    info('Phase 5 - Querying tables in DB')

conn.close()
