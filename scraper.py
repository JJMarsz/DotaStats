import urllib.request
import requests
import sys
import sqlite3
import json

# Control flags
fail_error = 0
log_lvl = 0
cache_data = 1

# Some useful constants
start_2018 = 1513728000
od = 'https://api.opendota.com/api/'
hdr = { 'User-Agent' : 'im a robot beepboop' }
heroes = ['Abaddon', 'Alchemist', 'Axe', 'Beastmaster', 'Brewmaster', 'Bristleback', 'Centaur Warrunner', 'Chaos Knight', 
          'Clockwerk', 'Doom', 'Dragon Knight', 'Earth Spirit', 'Earthshaker', 'Elder Titan', 'Huskar', 'Io', 'Kunkka', 
          'Legion Commander', 'Lifestealer', 'Lycan', 'Magnus', 'Mars', 'Night Stalker', 'Omniknight', 'Phoenix', 'Pudge', 
          'Sand King', 'Slardar', 'Spirit Breaker', 'Sven', 'Tidehunter', 'Timbersaw', 'Tiny', 'Treant Protector', 'Tusk', 
          'Underlord', 'Undying', 'Wraith King', 'Anti-Mage', 'Arc Warden', 'Bloodseeker', 'Bounty Hunter', 'Broodmother', 
          'Clinkz', 'Drow Ranger', 'Ember Spirit', 'Faceless Void', 'Gyrocopter', 'Juggernaut', 'Lone Druid', 'Luna', 'Medusa',
          'Meepo', 'Mirana', 'Monkey King', 'Morphling', 'Naga Siren', 'Nyx Assassin', 'Pangolier', 'Phantom Assassin', 
          'Phantom Lancer', 'Razor', 'Riki', 'Shadow Fiend', 'Slark', 'Sniper', 'Spectre', 'Templar Assassin', 'Terrorblade',
          'Troll Warlord', 'Ursa', 'Vengeful Spirit', 'Venomancer', 'Viper', 'Weaver', 'Ancient Apparition', 'Bane', 'Batrider', 
          'Chen', 'Crystal Maiden', 'Dark Seer', 'Dark Willow', 'Dazzle', 'Death Prophet', 'Disruptor', 'Enchantress', 'Enigma',
          'Grimstroke', 'Invoker', 'Jakiro', 'Keeper of the Light', 'Leshrac', 'Lich', 'Lina', 'Lion', 'Nature\'s Prophet', 
          'Necrophos',' Ogre Magi', 'Oracle', 'Outworld Devourer', 'Puck', 'Pugna', 'Queen of Pain', 'Rubick', 'Shadow Demon', 
          'Shadow Shaman', 'Silencer', 'Skywrath Mage', 'Storm Spirit', 'Techies', 'Tinker', 'Visage', 'Warlock', 'Windranger',
          'Winter Wyvern', 'Witch Doctor', 'Zeus']

#
# Subroutines
#
def error(msg):
    print('[ERROR] :: ' + msg)
    if fail_error:
        sys.exit()

def log(msg):
    print('[INFO] :: ' + msg)

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
conn = sqlite3.connect("stats.db")
cur = conn.cursor()
# loop on each team
for k in params.keys():
	match_ids = getMatchLinks(k, params[k], key)
	for match_id in match_ids:
		match_data = apiCall('matches/' + str(match_id), key)
		player_dp = {}

		# gather match stats
		match_dp = [int(match_data['series_id']), int(match_data['match_id']), int(match_data['radiant_win']), int(match_data['dire_team_id']), \
					int(match_data['radiant_team_id']), int(match_data['duration']), match_data['league']['name'], int(match_data['start_time'])-start_2018]
		print(match_dp)

		# gather player stats
		for player in match_data['players']:
			player_dp[player['account_id']] = [int(match_data['match_id']), int(player['hero_id']), int(player['kills']), int(player['deaths']), int(player['last_hits']) + \
												int(player['denies']), int(player['gold_per_min']), int(player['tower_kills']), \
												int(player['roshan_kills']), float(player['teamfight_participation']), int(player['obs_placed']), \
												int(player['camps_stacked']), int(player['rune_pickups']), int(player['firstblood_claimed']), float(player['stuns'])]
			print(str(player['account_id']) + " : " + str(player_dp[player['account_id']]))

		# BREAK EARLY TO LIMIT API CALLS FOR NOW

		break
