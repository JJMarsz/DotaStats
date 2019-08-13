import urllib.request
import requests
import sys
import sqlite3
import json
import math
from datetime import datetime

# Control variables
fail_error = 1          # fail on the first error
ti_mode = 1             # tournament counts only if matches atleast within 5 days of eachother (removes qualifier errors)
test_mode = 1           # uses different DB
log_lvl = 2             # 0-nothing, 1-ERROR,2-INFO,3-DEBUG
exec_phase = [1,2,3,4,5,6]        # 1, 2, 3, 4, 5, 6
curr_utc = 1564352276

# File locations
f_db = 'stats.db'
f_params = 'params.txt'
f_matches = 'matches.txt'

# Useful constants
day_length = 86400
od = 'https://api.opendota.com/api/'
hdr = { 'User-Agent' : 'im a robot beepboop' }
roles = {'1' : 'Core', '2' : 'Support', '4' : 'Mid'}
points = {'kills' : 0.3, 'deaths' : -0.3, 'lh_and_d' : 0.003, 'gpm' : 0.002, 'tower_kills' : 1, 'roshan_kills' : 1, 'teamfight' : 3, \
            'obs_placed' : 0.5, 'camps_stacked' : 0.5, 'rune_pickups' : 0.25, 'first_blood' : 4, 'stuns' : 0.05}# Deaths +3
best_of = {'1':1, '2':2, '3':2, '5':3}


###################
#                 #
#  Log Functions  #
#                 #
###################


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


################
#              #
#  One-Liners  #
#              #
################


# appends key to string and ap calls opendota
def apiCall(url, key): return json.loads(requests.get(od + url + '?api_key=' + key, headers=hdr).text)
# splits names
def splitName(names): return [names[:names.find('/')], names[names.find('/')+1:]]
# returns a unique hash of two teams
def fetchTeams(scenario): return ''.join(sorted(scenario.replace(' beats ', '')))


#########################
#                       #
#  Parameter Functions  #
#                       #
#########################


# parse input params
# Format:
# key-string
# num_tournaments
# team_id1 
# team_id2 
# ...
# team_idN 
# (MUST HAVE NEW LINE AT END)
def parseParams(params):
    dic = {'key' : params[:params.find('\n')]}
    params = params[params.find('\n')+1:]
    dic['num_tourny'] = params[:params.find('\n')]
    params = params[params.find('\n')+1:]
    dic['teams'] = []
    while params.find('\n') != -1:
        dic['teams'].append(params[:params.find('\n')])
        params = params[params.find('\n')+1:]
    return dic

# parse next day matches
# Format:
# team_tag1 team_tag2 (BO#) <-- Normal Case
# team_tag3 team_tag4 (BO#)
# team_tag5 team_tag3/team_tag4 (BO#)  <-- Case where opponent is winner of another series
# ...
# team_tagN team_tag(N+1) (BO#)
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


#############################
#                           #
#  Fantasy Point Functions  #
#                           #
#############################


# fetches FP stats from player_data based on conditons in query_tail and using params
def fetchFPStats(queryTail, params):
    cur.execute('SELECT kills, deaths, lh_and_d, gpm, tower_kills, roshan_kills, teamfight, obs_placed, camps_stacked, rune_pickups, \
            first_blood, stuns, duration FROM ' + queryTail, params)
    raw_data = cur.fetchall()
    retlist = []
    for line in raw_data:
        retlist.append([aggFP(line), (60*aggFP(line))/line[12]] + list(line))
    return retlist

# optains fp for each bonus stat and returns a list of results
def getFPBonusStats(stats):
    return [int(stats[0])*points['kills'], int(stats[1])*points['deaths'] + 3, \
    int(stats[2])*points['lh_and_d'], int(stats[3])*points['gpm'], int(stats[4])*points['tower_kills'], \
    int(stats[5])*points['roshan_kills'], float(stats[6])*points['teamfight'], int(stats[7])*points['obs_placed'], \
    int(stats[8])*points['camps_stacked'], int(stats[9])*points['rune_pickups'], int(stats[10])*points['first_blood'], \
    float(stats[11])*points['stuns']]

# sums all of the bonus stats into a single data point
def aggFP(stat):
    val = 0
    l = getFPBonusStats(stat)
    for i in l: val += i
    return val

# inserts FPpM rank using repeatable process
def insertFPpMRank(player, opp, table, bo, fppm, scenario=None):
    cur.execute('SELECT ts.avg_duration FROM team_summary AS ts, team_lookup AS tl WHERE tl.name = ts.name AND tl.tag IN (?,?)', [player[1], opp])
    ts = cur.fetchall()
    durs = extractColumn(ts, 0)
    length = avg([secToMin(timeToSec(durs[0])), secToMin(timeToSec(durs[1]))])
    if len(fppm)==0 or 'unk_' in table:
        cur.execute('INSERT INTO ' + table + ' VALUES (?,?,?,?,?,?,?)', [player[0], roles[player[2]], bo, bo*player[7]*length, bo*player[8]*length, \
            bo*player[9]*length, scenario])
    else:            
        cur.execute('UPDATE ' + table + ' SET num_games  = ?, high_fp = ?, avg_fp = ?, low_fp = ? WHERE name = ?', [bo + fppm[0][2], fppm[0][3] + bo*player[7]*length, \
            fppm[0][4] + bo*player[8]*length, fppm[0][5] + bo*player[9]*length, player[0]])

# aggregates the max values of a fp list
def aggMax(l):
    sel = best_of[str(len(l))]
    ret_val = 0;
    for i in range(sel):
        ret_val += max(l)
        l[l.index(max(l))] = -1
    return ret_val


####################
#                  #
#  Time Functions  #
#                  #
####################


# converts seconds to hh:mm:ss
def secToTime(sec):
    s = str(int(sec % 60))
    h = int(sec / 3600)
    if len(s) == 1: s = '0' + s
    if h > 0: 
        m = str(int((sec%3600)/60))
        if len(m) == 1: m = '0' + m
        return str(h) + ':' + m + ':' + s
    else: return str(int(sec/60)) + ':' + s

# converts hh:mm:ss to seconds
def timeToSec(time):
    time = time.strip()
    if len(time) > 5: 
        return int(time[6:8]) + int(time[3:5])*60 + int(time[0:2])*3600
    else: 
        return int(time[3:5]) + int(time[0:2])*60

# converts seconds to minutes
def secToMin(sec):
    return round(sec/60)

# using curr_utc or specified utc, fetch converted utc time
def normalizeTime(time=0):
    if not time: 
        if not curr_utc: time = (datetime.utcnow()-datetime(1970,1,1)).total_seconds()
        else: time = curr_utc
    time -= (time % day_length)# get start of day utc time
    time += day_length/3 # add timezone offset
    return time


#############################
#                           #
#  List/Dict/SQL Functions  #
#                           #
#############################

# returns upper bound on number of matches
def getMatchLinks(team_id, num_tournys, key):
    match_json = apiCall('teams/' + team_id + '/matches', key)
    match_links = []
    t_count = 0
    last_t = ''
    last_start = 0
    for i in range(int(num_tournys)*50):
        if i >= len(match_json):
            break
        if match_json[i]['league_name'] != last_t or (int(match_json[i]['start_time'])+5*day_length < last_start and ti_mode):
            last_t = match_json[i]['league_name']
            t_count += 1
            if t_count > int(num_tournys):
                break
        last_start = int(match_json[i]['start_time'])
        match_links.append(match_json[i]['match_id'])
    return match_links


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

# load ranking data for specified table and type
def loadRanks(table, col, role, max_bo, ranks):
    cur.execute('SELECT name, ' + col + ' FROM ' + table + ' WHERE num_games = ? AND role = ? ORDER BY ' + col + ' ASC', [max_bo, roles[role]])
    single_ranks = cur.fetchall()
    for i in range(len(single_ranks)):
        if single_ranks[i][0] in ranks[roles[role]]:
            ranks[roles[role]][single_ranks[i][0]] += i+1
        else:
            ranks[roles[role]][single_ranks[i][0]] = i+1

# header for summary table reseting
def summaryHeader(table_name):
    cur.execute('SELECT * FROM ' + table_name)
    if len(cur.fetchall()) > 0:
        info('Flushing ' + table_name + ' table...')
        cur.execute('DELETE FROM ' + table_name)
        conn.commit()
    info('Populating '+ table_name + ' table...')


####################
#                  #
#  Math Functions  #
#                  #
####################


# returns average of on array
def avg(arr):
    val = 0
    for i in arr:
        val += i
    return val/len(arr)

#returns std dev of array
def stdDev(arr):
    mean = avg(arr)
    var = 0
    for i in arr:
        var += (i-mean)*(i-mean)
    return math.sqrt(var/len(arr))


##########
#        #
#  MAIN  #
#        #
##########


# input validation
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
    for k in params['teams']:
        info('Scraping team ' + k + '\'s matches')
        match_ids = getMatchLinks(k, params['num_tourny'], key)
        info('Discovered ' + str(len(match_ids)) + ' matches in last ' + params['num_tourny'] + ' tournaments...')
        r = 0
        for match_id in match_ids:
            cur.execute('SELECT * FROM match_data WHERE match_id = ?', [match_id,])
            if len(cur.fetchall()) == 0:
                match_data = apiCall('matches/' + str(match_id), key)
                player_dp = {}

                # gather match stats
                match_dp = [int(match_data['match_id']), int(match_data['series_id']), int(match_data['radiant_win']), int(match_data['dire_team_id']), \
                            int(match_data['radiant_team_id']), int(match_data['duration']), match_data['league']['name'], int(match_data['start_time']), None]
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
    for k in params['teams']:
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
            std_dev_fppm, avg_fppm + std_dev_fppm, avg_fppm, avg_fppm - std_dev_fppm] + getFPBonusStats(fp_stats))
    conn.commit()

    summaryHeader('role_summary')
    for role in roles.keys():
        stats = fetchFPStats('player_data AS pd, match_data AS md, player_lookup AS pl WHERE md.match_id = pd.match_id AND pd.account_id =  pl.account_id AND role = ?', [role,])
        avg_fp = avg(extractColumn(stats, 0))
        avg_fppm = avg(extractColumn(stats, 1))
        fp_stats = []
        for i in range(len(stats[0])-2):
            fp_stats.append(avg(extractColumn(stats, i+2)))
        cur.execute('INSERT INTO role_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [roles[role], avg_fp, avg_fppm] + getFPBonusStats(fp_stats))
    conn.commit()

    summaryHeader('hero_summary')
    cur.execute('SELECT * FROM hero_lookup')
    heroes = cur.fetchall()
    for hero in heroes:
        stats = fetchFPStats('player_data AS pd, hero_lookup AS hl, match_data AS md WHERE md.match_id = pd.match_id AND pd.hero_id = hl.hero_id AND pd.hero_id = ?', [hero[0],])
        if not len(stats):
            info('No games played on ' + hero[1])
            continue
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
    info('Phase 5 - Generating rankings')
    match_file = open(f_matches, 'r')
    matches = parseMatches(match_file);
    match_file.close()
    cur.execute('DELETE FROM fp_rankings')
    cur.execute('DELETE FROM fppm_rankings')
    cur.execute('DELETE FROM unk_fp_stats')
    cur.execute('DELETE FROM unk_fppm_stats')
    cur.execute('SELECT pl.name, tl.tag, pl.role, ts.avg_duration, ps.high_fp, ps.avg_fp, ps.low_fp, ps.high_fppm, ps.avg_fppm, ps.low_fppm FROM player_lookup AS pl, team_lookup AS tl, \
        player_summary AS ps, team_summary AS ts WHERE tl.name = ts.name AND tl.team_id = pl.team_id AND ps.player_name = pl.name')
    players = cur.fetchall()
    max_bo = 0
    for player in players:
        for team in matches.keys():
            if player[1] in team:
                match_data =  matches[team]
                for match in match_data['matches']:
                    tables = ['fp_rankings', 'fppm_rankings']
                    opp = list(match)[0]
                    bo = match[list(match)[0]]
                    if '/' in opp or '/' in team: tables = ['unk_fp_stats', 'unk_fppm_stats']#if opp or player's team is unknown
                    cur.execute('SELECT * FROM ' + tables[0] + ' WHERE name = ?', [player[0]])
                    fp = cur.fetchall()
                    if len(fp) == 0:
                        cur.execute('INSERT INTO '+ tables[0] + ' VALUES (?,?,?,?,?,?,?)', [player[0], roles[player[2]], bo, bo*player[4], bo*player[5], bo*player[6], None])
                    else:
                        cur.execute('UPDATE ' + tables[0] + ' SET num_games = ?, high_fp = ?, avg_fp = ?, low_fp = ? WHERE name = ?', [bo + fp[0][2], fp[0][3] + bo*player[4], \
                            fp[0][4] + bo*player[5], fp[0][5] + bo*player[6], player[0]])
                    cur.execute('SELECT * FROM ' + tables[1] + ' WHERE name = ?', [player[0]])
                    fppm = cur.fetchall()
                    # check if a team needs to be split
                    if 'unk_' in tables[0]:
                        if '/' in team:
                            teams = splitName(team)
                            if player[1] == teams[0]: prev_op = teams[1]
                            else: prev_op = teams[0]
                            cur.execute('UPDATE unk_fp_stats SET scenario = ? WHERE name = ?', [player[1] + ' beats ' + prev_op, player[0]])
                            insertFPpMRank(player, opp, tables[1], bo, fppm, player[1] + ' beats ' + prev_op)
                        elif '/' in opp:
                            teams = splitName(opp)
                            insertFPpMRank(player, teams[0], tables[1], bo, fppm, teams[0] + ' beats ' + teams[1])
                            insertFPpMRank(player, teams[1], tables[1], bo, fppm, teams[1] + ' beats ' + teams[0])
                        else: error('Labeled as unknown matchup, yet niether team meets that criterea')
                    else: insertFPpMRank(player, opp, tables[1], bo, fppm)
    conn.commit()
    # check whether scenarios should be made
    cur.execute('SELECT ufppms.*, ufps.high_fp, ufps.avg_fp, ufps.low_fp FROM unk_fppm_stats as ufppms, unk_fp_stats AS ufps WHERE ufppms.name = ufps.name AND \
        (ufppms.scenario = ufps.scenario OR ufps.scenario IS NULL)')
    unks = cur.fetchall()

    if len(unks) > 0:
        scenarios = {}# [pair][path][player_name][fp,fppm,bo]
        for unk in unks:
            teams = fetchTeams(unk[6])
            if teams not in scenarios.keys():
                scenarios[teams] = {unk[6] : {unk[0] : {'fp' : [unk[7:10]], 'fppm' : [unk[3:6]], 'bo' : unk[2]}}}
            elif unk[6] not in scenarios[teams].keys():
                scenarios[teams][unk[6]] = {unk[0] : {'fp' : [unk[7:10]], 'fppm' : [unk[3:6]], 'bo' : unk[2]}}
            elif unk[0] not in scenarios[teams][unk[6]].keys():
                scenarios[teams][unk[6]][unk[0]] = {'fp' : [unk[7:10]], 'fppm' : [unk[3:6]], 'bo' : unk[2]}
            else: error('Already have an entry for ' + unk[0] + ' in scenario ' + teams + ' path ' + unk[6])
        #enumerate the possibilities
        paths = []
        for scenario in scenarios.keys():
            paths.append(list(scenarios[scenario].keys()))
        variations = []
        for i in range((2**len(paths))):
            variations.append([])
        for i in range(len(variations)):#total amount of variations
            count = i
            for j in range(len(paths)):#go through every path-set
                for k in range(len(paths[j])):#select a single path from each path_set
                    if (len(paths[j]) - k - 1)*(2**(len(paths) - j - 1)) <= count:
                        variations[i].append(paths[j][k])
                        count -= (len(paths[j]) - k - 1)*(2**(len(paths) - j - 1))
                        break
        cur.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
        tables = cur.fetchall()
        for table in tables:
        	if 'fp_rankings_' in table[0] or 'fppm_rankings_' in table[0]:
        		cur.execute('DROP TABLE ' + table[0])
        #create len(variations) amount of tables and import the appropriate data into it
        for var in variations:
        	name = ''
        	for scen in var: name += scen.replace('.', 'o').replace(' ', '_')
        	cur.execute('CREATE TABLE fp_rankings_' + name + ' AS SELECT * FROM fp_rankings WHERE 0')
        	cur.execute('INSERT INTO fp_rankings_' + name + ' SELECT * FROM fp_rankings')
        	cur.execute('CREATE TABLE fppm_rankings_' + name + ' AS SELECT * FROM fppm_rankings WHERE 0')
        	cur.execute('INSERT INTO fppm_rankings_' + name + ' SELECT * FROM fppm_rankings')
        	for scenario in scenarios.keys():
        		for path in scenarios[scenario].keys():
        			if path in var:
        				for player in scenarios[scenario][path].keys():
        					cur.execute('SELECT * FROM fp_rankings_' + name + ' WHERE name = ?', [player,])
        					fp = cur.fetchall()
        					cur.execute('UPDATE fp_rankings_' + name + ' SET num_games = ?, high_fp = ?, avg_fp = ?, low_fp = ? WHERE name = ?', \
        						[fp[0][2] + scenarios[scenario][path][player]['bo'], \
        						fp[0][3] + scenarios[scenario][path][player]['bo']*scenarios[scenario][path][player]['fp'][0][0], \
                            	fp[0][4] + scenarios[scenario][path][player]['bo']*scenarios[scenario][path][player]['fp'][0][1], \
                            	fp[0][5] + scenarios[scenario][path][player]['bo']*scenarios[scenario][path][player]['fp'][0][2], player])
        					cur.execute('SELECT * FROM fppm_rankings_' + name + ' WHERE name = ?', [player,])
        					fppm = cur.fetchall()
        					cur.execute('UPDATE fppm_rankings_' + name + ' SET num_games = ?, high_fp = ?, avg_fp = ?, low_fp = ? WHERE name = ?', \
        						[fppm[0][2] + scenarios[scenario][path][player]['bo'], \
        						fppm[0][3] + scenarios[scenario][path][player]['bo']* scenarios[scenario][path][player]['fppm'][0][0], \
                            	fppm[0][4] + scenarios[scenario][path][player]['bo']*scenarios[scenario][path][player]['fppm'][0][1], \
                            	fppm[0][5] + scenarios[scenario][path][player]['bo']*scenarios[scenario][path][player]['fppm'][0][2], player])

    else: info('No branching scenarios detected')


    #TODO MAKE RANKINGS FOR EACH SCENARIO
    # Now create a super ranking for each player playing the most amount of matches for each role
    cur.execute('DELETE FROM rankings')
    cur.execute('SELECT MAX(num_games) FROM fp_rankings')
    max_bo = cur.fetchall()[0][0]
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

if 6 in exec_phase:
    info('Phase 6 - Assessing a day of FP performance')
    cur.execute('DELETE FROM match_day_summary')
    #first normalize current utc time to time zone of ti
    start_today = normalizeTime()
    start_yest = start_today - day_length
    # retrieve all games in this range
    cur.execute('SELECT * FROM match_data WHERE start_time < ? AND start_time > ?', [start_today, start_yest,])
    matches = cur.fetchall()
    player_data = {}
    for match in matches:
        cur.execute('SELECT series_id, pl.name, pl.role, kills, deaths, lh_and_d, gpm, tower_kills, roshan_kills, teamfight, obs_placed, camps_stacked, rune_pickups, first_blood, stuns \
            FROM match_data AS md, player_data AS pd, player_lookup AS pl WHERE pd.match_id = md.match_id AND pl.account_id = pd.account_id AND pd.match_id = ?', [match[0]])
        players = cur.fetchall()
        
        for player in players:
            if player[1] not in player_data.keys(): player_data[player[1]] = {}
            if player[0] not in player_data[player[1]].keys(): player_data[player[1]][player[0]] = []
            player_data[player[1]]['role'] = player[2]
            player_data[player[1]][player[0]].append(aggFP(player[3:]))
    
    # now select max amount
    for player in player_data.keys():
        for series in player_data[player].keys():
            if series != 'role':
                cur.execute('INSERT INTO match_day_summary VALUES (?,?,?)', [player, player_data[player]['role'], aggMax(player_data[player][series])])


    conn.commit()

info('Shutting down...')

conn.close()
