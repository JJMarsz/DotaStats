import bs4 as bs
import urllib.request
import requests
import sys
import sqlite3
import time
import os.path
from os import path
import random
import json

# Control flags
fail_error = 0
log_lvl = 0
cache_data = 1

# Some useful constants
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
def getMatchLinks(team_id, num_series, key):
    match_json = apiCall('teams/' + team_id + '/matches', key)
    match_links = []
    for i in range(int(num_series)*4):
    	match_links.append(match_json[i]['match_id'])
    return match_links

# parse input params
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
	series_ids = []
	for match_id in match_ids:
		match_data = apiCall('matches/' + str(match_id), key)

		# gather match stats


		#gather player stats


		# early break condiiton
		if match_data['series_id'] not in series_ids:
			if len(series_ids) == int(params[k]):
				break
			series_ids.append(match_data['series_id'])
