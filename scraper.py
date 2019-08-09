import urllib.request
import requests
import sys
import sqlite3
import json
import math

# Control variables
fail_error = 0
ti_mode = 1 # only scrape macthes after a specific date
test_mode = 0 # uses different DB
log_lvl = 2 #0-nothing, 1-ERROR,2-INFO,3-DEBUG
exec_phase = [6] # 1, 2, 3, 4, 5, 6, 7
f_db = 'stats.db'
f_params = 'params.txt'
f_matches = 'matches.txt'

# Some useful constants
start_2018 = 1513728000
ti_start = 1547095680
od = 'https://api.opendota.com/api/'
hdr = { 'User-Agent' : 'im a robot beepboop' }
roles = {'1' : 'Core', '2' : 'Support', '4' : 'Mid'}
points = {'kills' : 0.3, 'deaths' : -0.3, 'lh_and_d' : 0.003, 'gpm' : 0.002, 'tower_kills' : 1, 'roshan_kills' : 1, 'teamfight' : 3, \
            'obs_placed' : 0.5, 'camps_stacked' : 0.5, 'rune_pickups' : 0.25, 'first_blood' : 4, 'stuns' : 0.05}# Deaths +3
best_of = {'1':1, '2':2, '3':2, '5':3}

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
        if match_json['start_time'] < ti_start and ti_mode:
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
    
# parse next day matches
# Format:
# team_tag1 team_tag2 (BO#) <-- Normal Case
# team_tag3 team_tag4 (BO#)
# team_tag5 team_tag3/team_tag4 (BO#)  <-- Case where opponent is winner of another series
# ...
# (MUST HAVE NEW LINE AT END)
def parseMatches(match_file):
    cur.execute('SELECT * FROM team_lookup')
    teams = cur.fetchall()
    tags = extractColumn(teams, 2)
    dic = {}
    for match in match_file:
        bo = best_of[match[match.rfind(' ')+1:len(match)-1]]
        if match[:match.find(' ')] in tags:
            if match[:match.find(' ')] not in dic.keys(): dic[match[:match.find(' ')]] = {'total' : bo, 'matches' : []}
            else: dic[match[:match.find(' ')]]['total'] += bo
        else: error('Team tag ' + match[:match.find(' ')] + ' is not a valid TI team')
        if match[match.find(' ')+1:match.rfind(' ')] in tags:
            if match[match.find(' ')+1:match.rfind(' ')] not in dic.keys(): dic[match[match.find(' ')+1:match.rfind(' ')]] = {'total' : bo, 'matches' : []}
            else: dic[match[match.find(' ')+1:match.rfind(' ')]]['total'] += bo
        elif '/' in match[match.find(' ')+1:match.rfind(' ')]: dic[match[match.find(' ')+1:match.rfind(' ')]] = {'total' : bo, 'matches' : []}
        else: error('Team tag ' + match[match.find(' ')+1:match.rfind(' ')] + ' is not a valid TI team')
        dic[match[:match.find(' ')]]['matches'].append({match[match.find(' ')+1:match.rfind(' ')] : bo})
        dic[match[match.find(' ')+1:match.rfind(' ')]]['matches'].append({match[:match.find(' ')] : bo})

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

def secToTime(sec):
    s = str(int(sec % 60))
    h = int(sec / 3600)
    if len(s) == 1: s = '0' + s
    if h > 0: 
    	m = str(int((sec%3600)/60))
    	if len(m) == 1: m = '0' + m
    	return str(h) + ':' + m + ':' + s
    else: return str(int(sec/60)) + ':' + s

def timeToSec(time):
    time = time.strip()
    if len(time) > 5: 
        return int(time[6:8]) + int(time[3:5])*60 + int(time[0:2])*3600
    else: 
        return int(time[3:5]) + int(time[0:2])*60

def secToMin(sec):
	return round(sec/60)

def summaryHeader(table_name):
    cur.execute('SELECT * FROM ' + table_name)
    if len(cur.fetchall()) > 0:
        info('Flushing ' + table_name + ' table...')
        cur.execute('DELETE FROM ' + table_name)
        conn.commit()
    info('Populating '+ table_name + ' table...')

def fetchFPStats(queryTail, params):
    cur.execute('SELECT kills, deaths, lh_and_d, gpm, tower_kills, roshan_kills, teamfight, obs_placed, camps_stacked, rune_pickups, \
            first_blood, stuns, duration FROM ' + queryTail, params)
    raw_data = cur.fetchall()
    retlist = []
    for line in raw_data:
        retlist.append([fp(line), (60*fp(line))/line[12]] + list(line))
    return retlist

def fp(stats):
    stat_dp = stats[0][0]*points['kills'] + stats[0][1]*points['deaths'] + 3 + stats[0][2]*points['lh_and_d'] + stats[0][3]*points['gpm'] + \
            stats[0][4]*points['tower_kills'] + stats[0][5]*points['roshan_kills'] + stats[0][6]*points['teamfight'] + stats[0][7]*points['obs_placed'] + \
            stats[0][8]*points['camps_stacked'] + stats[0][9]*points['rune_pickups'] + stats[0][10]*points['first_blood'] + stats[0][11]*points['stuns']
    stats[0] = list(stats[0])
    for i in range(len(stats[0])):
        stats[0][i] = round(stats[0][i], 4)
    stat_dp = round(stat_dp, 4)
    return stat_dp

def avg(arr):
    val = 0
    for i in arr:
        val += i
    return val/len(arr)

def stdDev(arr):
    mean = avg(arr)
    var = 0
    for i in arr:
        var += (i-mean)*(i-mean)
    return math.sqrt(var/len(arr))

def flareData(stats):
    return [stats[0]*points['kills'], stats[1]*points['deaths'] + 3, \
    stats[2]*points['lh_and_d'], stats[3]*points['gpm'], stats[4]*points['tower_kills'], \
    stats[5]*points['roshan_kills'], stats[6]*points['teamfight'], stats[7]*points['obs_placed'], \
    stats[8]*points['camps_stacked'], stats[9]*points['rune_pickups'], stats[10]*points['first_blood'], \
    stats[11]*points['stuns']]

def fp(stat):
    return stat[0]*points['kills'] + stat[1]*points['deaths'] + 3 + stat[2]*points['lh_and_d'] + stat[3]*points['gpm'] + \
            stat[4]*points['tower_kills'] + stat[5]*points['roshan_kills'] + stat[6]*points['teamfight'] + stat[7]*points['obs_placed'] + \
            stat[8]*points['camps_stacked'] + stat[9]*points['rune_pickups'] + stat[10]*points['first_blood'] + stat[11]*points['stuns']

def loadRanks(table, col, role, max_bo, ranks):
    cur.execute('SELECT name, ' + col + ' FROM ' + table + ' WHERE num_games = ? AND role = ? ORDER BY ' + col + ' ASC', [max_bo, roles[role]])
    single_ranks = cur.fetchall()
    for i in range(len(single_ranks)):
        if single_ranks[i][0] in ranks[roles[role]]:
            ranks[roles[role]][single_ranks[i][0]] += i+1
        else:
            ranks[roles[role]][single_ranks[i][0]] = i+1

def splitName(names):
	return [names[:names.find('/')], names[names.find('/')+1:]]


#    
# Main
#

# Validate args
if(len(sys.argv) > 1):
    error("Too many arguments :: Format is python scraper.py")
    sys.exit()
if(len(sys.argv) < 0):
    error("Too little arguments :: Format is python scraper.py")
    sys.exit()
param_file = open(f_params, 'r')
params = parseParams(param_file.read());
param_file.close()
key = params['key']
params.pop('key')
# Connect to the Database
info('Connecting to the database...')
conn = ''
if test_mode: conn = sqlite3.connect('test_' + f_db)
else: conn = sqlite3.connect(f_db)
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

    # create lookups for TI players
    info('Generating player_lookup')
    cur.execute('SELECT MAX(start_time), tl.team_id, md.match_id FROM match_data AS md, team_lookup AS tl WHERE md.dire_team_id = tl.team_id GROUP BY tl.team_id')
    dire_data = cur.fetchall()
    cur.execute('SELECT MAX(start_time), tl.team_id, md.match_id FROM match_data AS md, team_lookup AS tl WHERE md.radiant_team_id = tl.team_id GROUP BY tl.team_id')
    radiant_data = cur.fetchall()
    pros = apiCall('/proPlayers', key)
    recent_matches = []
    for i in range(len(radiant_data)):
        if radiant_data[i][0] > dire_data[i][0]: recent_matches.append([radiant_data[i][1],radiant_data[i][2], 1])
        else: recent_matches.append([dire_data[i][1],dire_data[i][2], 0])
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
    if len(loaded_heroes) == len(heroes):
        info('Already have every hero')
    else:
        for hero in heroes:
            if hero['id'] not in loaded_heroes:
                cur.execute('INSERT INTO hero_lookup VALUES (?,?,?)',[hero['id'],hero['localized_name'],hero['legs'],])
                info('Inserted ' + hero['localized_name'])

if 3 in exec_phase:
    info('Phase 3 - Trimming and Appending data tables')
    info('Deleting non-TI player\'s data...')
    cur.execute('DELETE FROM player_data WHERE account_id NOT IN (SELECT account_id FROM player_lookup)')
    conn.commit()

if 4 in exec_phase:
    info('Phase 4 - Aggregating data into summary tables')
    summaryHeader('player_summary')
    cur.execute('SELECT * FROM player_lookup')
    players = cur.fetchall()
    for player in players:
        stats = fetchFPStats('player_data AS pd, match_data AS md WHERE md.match_id = pd.match_id AND pd.account_id = ?', [player[0],])
        std_dev_fp = stdDev(extractColumn(stats, 0))
        std_dev_fppm = stdDev(extractColumn(stats, 1))
        avg_fp = avg(extractColumn(stats, 0))
        avg_fppm = avg(extractColumn(stats, 1))
        fp_stats = []
        for i in range(len(stats[0])-2):
            fp_stats.append(avg(extractColumn(stats, i+2)))
        #wins = fetchAvgStats('player_data AS pd, match_data AS md, team_lookup AS tl, player_lookup AS pl WHERE \
        #    ((md.radiant_win = 1 and md.radiant_team_id = tl.team_id) OR (md.radiant_win = 0 and md.dire_team_id = tl.team_id)) \
        #    AND md.match_id = pd.match_id AND tl.team_id = pl.team_id AND pd.account_id = pl.account_id AND pd.account_id = ?', [player[0],])
        #win_dp = getAvgDataPoint(wins)
        #losses = fetchAvgStats('player_data AS pd, match_data AS md, team_lookup AS tl, player_lookup AS pl WHERE  \
        #    ((md.radiant_win = 0 and md.radiant_team_id = tl.team_id) OR (md.radiant_win = 1 and md.dire_team_id = tl.team_id)) \
        #    AND md.match_id = pd.match_id AND tl.team_id = pl.team_id AND pd.account_id = pl.account_id AND pd.account_id = ?', [player[0],])
        #loss_dp = getAvgDataPoint(losses)
        cur.execute('INSERT INTO player_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [player[1], roles[player[3]], std_dev_fp, avg_fp + std_dev_fp, avg_fp, avg_fp - std_dev_fp, \
            std_dev_fppm, avg_fppm + std_dev_fppm, avg_fppm, avg_fppm - std_dev_fppm] + flareData(fp_stats))
    conn.commit()

    summaryHeader('role_summary')
    for role in roles.keys():
        stats = fetchFPStats('player_data AS pd, match_data AS md, player_lookup AS pl WHERE md.match_id = pd.match_id AND pd.account_id =  pl.account_id AND role = ?', [role,])
        avg_fp = avg(extractColumn(stats, 0))
        avg_fppm = avg(extractColumn(stats, 1))
        fp_stats = []
        for i in range(len(stats[0])-2):
            fp_stats.append(avg(extractColumn(stats, i+2)))
        cur.execute('INSERT INTO role_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [roles[role], avg_fp, avg_fppm] + flareData(fp_stats))
    conn.commit()

    summaryHeader('hero_summary')
    cur.execute('SELECT * FROM hero_lookup')
    heroes = cur.fetchall()
    for hero in heroes:
        stats = fetchFPStats('player_data AS pd, hero_lookup AS hl, match_data AS md WHERE md.match_id = pd.match_id AND pd.hero_id = hl.hero_id AND pd.hero_id = ?', [hero[0],])
        std_dev_fp = stdDev(extractColumn(stats, 0))
        std_dev_fppm = stdDev(extractColumn(stats, 1))
        avg_fp = avg(extractColumn(stats, 0))
        avg_fppm = avg(extractColumn(stats, 1))
        fp_stats = []
        for i in range(len(stats[0])-2):
            fp_stats.append(avg(extractColumn(stats, i+2)))
        role_stats = []
        for role in roles.keys():
            cur.execute('SELECT COUNT(pl.role) FROM player_data AS pd, player_lookup AS pl WHERE pd.account_id = pl.account_id AND pl.role = ? AND pd.hero_id = ?', [role, hero[0],])
            role_stat = cur.fetchall()
            role_stats.append(role_stat[0][0])
        total = role_stats[0] + role_stats[1] + role_stats[2]
        cur.execute('INSERT INTO hero_summary VALUES (?,?,?,?,?,?,?)', [hero[1], avg_fp, avg_fppm, total, role_stats[0]/total, role_stats[2]/total, role_stats[1]/total])
    conn.commit()

    summaryHeader('team_summary')
    cur.execute('SELECT * FROM team_lookup')
    teams = cur.fetchall()

    for team in teams:
        cur.execute('SELECT tl.name, AVG(duration) FROM player_lookup AS pl, match_data AS md, player_data AS pd, team_lookup AS tl WHERE md.match_id = pd.match_id \
                                                                        AND pl.account_id = pd.account_id AND tl.team_id = pl.team_id AND pl.team_id = ?', [team[0]])
        stats_dp = cur.fetchall()
        cur.execute('INSERT INTO team_summary VALUES (?,?)', [stats_dp[0][0], secToTime(stats_dp[0][1])])

    conn.commit()

#    summaryHeader('leg_summary')
#    for i in [0,2,4,6,8]:
#        for role in roles.keys():
#            stats = fetchAvgStats('player_data AS pd, hero_lookup AS hl, player_lookup AS pl, match_data AS md WHERE pl.account_id = pd.account_id AND \
#                    md.match_id = pd.match_id AND pd.hero_id = hl.hero_id AND hl.legs = ? AND pl.role = ?', [i, role,])
#            if stats[0][0] is not None:
#                stat_dp = getAvgDataPoint(stats)
#                cur.execute('INSERT INTO leg_summary VALUES (?,?,?,?)', [roles[role], i, stats_dp, fppm('avg')])
#
#    conn.commit()

if 5 in exec_phase:
    info('Phase 5 - Scraping next days matches')

    conn.commit()

if 6 in exec_phase:
    info('Phase 6 - Generating rankings')
    match_file = open(f_matches, 'r')
    matches = parseMatches(match_file);
    match_file.close()
    cur.execute('DELETE FROM fp_rankings')
    cur.execute('DELETE FROM fppm_rankings')
    cur.execute('SELECT pl.name, tl.tag, pl.role, ts.avg_duration, ps.high_fp, ps.avg_fp, ps.low_fp, ps.high_fppm, ps.avg_fppm, ps.low_fppm FROM player_lookup AS pl, team_lookup AS tl, \
    	player_summary AS ps, team_summary AS ts WHERE tl.name = ts.name AND tl.team_id = pl.team_id AND ps.player_name = pl.name')
    players = cur.fetchall()
    max_bo = 0
    for player in players:
        for team in matches.keys():
            if player[1] in team:
                match_data =  matches[player[1]]
                for match in match_data['matches']:
                	tables = ['fp_rankings', 'fppm_rankings']
                    bo = match[list(match)[0]]
                    opp = list(match)[0]
                    if list(match)[0] not in match.keys() or player[1] == team: tables = ['unk_fp_stats', 'unk_fppm_stats']#if opp or player's team is unknown
                    if match_data['total'] > max_bo: max_bo = match_data['total']
                    cur.execute('SELECT * FROM ' + tables[0] + ' WHERE name = ?', [player[0]])
                    fp = cur.fetchall()
                    if len(fp) == 0:
                        cur.execute('INSERT INTO '+ tables[0] + ' VALUES (?,?,?,?,?,?)', [player[0], roles[player[2]], match_data['total'], bo*player[4], bo*player[5], bo*player[6]])
                    else:
                        cur.execute('UPDATE ' + tables[0] + ' SET high_fp = ?, avg_fp = ?, low_fp = ? WHERE name = ?', [fp[0][3] + bo*player[4], fp[0][4] + bo*player[5], fp[0][5] + bo*player[6],\
                        player[0]])
                    cur.execute('SELECT * FROM ' + tables[1] + ' WHERE name = ?', [player[0]])
                    fppm = cur.fetchall()
                    if 'unk_' in tables[0]:
                    	#find out which team needs to be split
                    	#update scenario
                    	cur.execute('UPDATE unk_fp_stats SET scenario = ? WHERE name = ?', [player[1] + ' beats ' + , player[0]])
                    else:
                    	team_tag = list(match)[0]
                    cur.execute('SELECT ts.avg_duration FROM team_summary AS ts, team_lookup AS tl WHERE tl.name = ts.name AND tl.tag IN (?,?)', [player[1], team_tag])
                    ts = cur.fetchall()
                    durs = extractColumn(ts, 0)
                    length = avg([secToMin(timeToSec(durs[0])), secToMin(timeToSec(durs[1]))])
                    if len(fppm) == 0:
                        cur.execute('INSERT INTO ' + tables[1] + ' VALUES (?,?,?,?,?,?)', [player[0], roles[player[2]], match_data['total'], bo*player[7]*length, bo*player[8]*length, \
                            bo*player[9]*length])
                    else:
                        cur.execute('UPDATE ' + tables[1] + ' SET high_fp = ?, avg_fp = ?, low_fp = ? WHERE name = ?', [fppm[0][3] + bo*player[7]*length, fppm[0][4] + bo*player[8]*length, \
                            fppm[0][5] + bo*player[9]*length, player[0]])

    # Now create a super ranking for each player playing the most amount of matches for each role
    cur.execute('DELETE FROM rankings')
    ranks = {'Core' : {}, 'Mid' : {}, 'Support' : {}} 
    for role in roles.keys():
        loadRanks('fp_rankings', 'high_fp', role, max_bo, ranks)
        loadRanks('fp_rankings', 'avg_fp', role, max_bo, ranks)
        loadRanks('fp_rankings', 'low_fp', role, max_bo, ranks)
        loadRanks('fppm_rankings', 'high_fp', role, max_bo, ranks)
        loadRanks('fppm_rankings', 'avg_fp', role, max_bo, ranks)
        loadRanks('fppm_rankings', 'low_fp', role, max_bo, ranks)

        for player in ranks[roles[role]].keys():
            cur.execute('INSERT INTO rankings VALUES (?,?,?)', [player, roles[role], ranks[roles[role]][player]])

    conn.commit()

if 7 in exec_phase:
    info('Phase 7 - Assessing last day FP performance')

    conn.commit()

info('Shutting down...')

conn.close()
