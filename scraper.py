import bs4 as bs
import urllib.request
import requests
import sys
import sqlite3
import time
import os.path
from os import path
import random

# Control flags
fail_error = 0
log_lvl = 0
cache_data = 1

# Some useful constants
db = 'https://www.dotabuff.com'
hdr = { 'User-Agent' : 'sorry for hitting your server a lot, im trying to cache some pages for testing to ease my burden on your server' }
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

# Sanitates the URL 
def cleanUrl(data):
    clean = {'?' : 'QMARKQ'}
    for k in clean.keys():
        if k in data:
            data = data.replace(k, clean[k])
    return (data + '.html')

# requests a soup of the url
def getSoup(url):
    # check cache if webpage was already downloaded
    if path.exists(cleanUrl(url[8:])):
        log('Cache hit on: ' + url)
        f = open(cleanUrl(url[8:]), "r", encoding='utf-8')
        source = f.read()
        return bs.BeautifulSoup(source, 'lxml')
    else:
        log('Trying to access web URL: ' + url)
        random.seed()
        time.sleep(5 + random.randrange(3,6))
        source = requests.get(url, headers=hdr)
        if source.status_code != 200:
            print(source.headers)
            error("Received error " + str(source.status_code) + ". Exiting...")
            sys.exit()
        else:
            exists = ''
            url = url[8:]
            log('Success!')
            while url.find('/') != -1:
                if not path.exists((exists + url[:url.find('/')+1])):
                    os.mkdir(exists + url[:url.find('/')+1])
                exists += url[:url.find('/')+1]
                url = url[url.find('/')+1:]
            f = open(cleanUrl(exists + url), "w+", encoding='utf-8')
            f.write(source.text)
            f.close()
        return bs.BeautifulSoup(source.text, 'lxml')

# retrieves the num_series amount of series_links
def getSeriesLinks(soup, num_series):
    series = []
    next_page = '2'
    next_page_link = ''
    while len(series) < int(num_series):
        for url in soup.find_all('a'):
            if '/esports/series/' in url.get('href'):
                series.append(db + url.get('href'))
            if 'page=' + next_page == url.get('href')[len(url.get('href'))-6:len(url.get('href'))]:
                next_page_link = db + url.get('href')
        #set up the variables for the next page
        soup = getSoup(next_page_link)
        next_page = str(int(next_page) + 1)
    return series

# determines the type of a series
def getBestOf(soup):
    bo = -1
    if 'Best of ' in soup.get_text():
        idx = soup.get_text().find('Best of ')
        bo = int(soup.get_text()[idx + 8])
    else:
        error('Cannot find the amount of games in the series')
    return bo

# returns the amount of matches in the series
def getMatchLinks(soup):
    match_links = []
    game_no = 1
    while ("Game " + str(game_no) + ":") in soup.getText():
        game_no += 1
    game_no -= 1
    for url in soup.find_all('a'):
        if '/matches/' in url.get('href'):
            match_links.append(db + url.get('href'))
    match_links = match_links[0:game_no]
    return match_links

# removes blacklisted words while maintaining whitelisted words
def blacklist(data):
    blacklist = ['Top', 'Bottom', 'Middle', 'Roaming', '(Off)', '(Safe)', 'won', 'lost', 'Core', 'Support', 'Jungle', 'Dire', 'Radiant', 'drew']
    whitelist = ['Topson']
    for b in blacklist:
        for w in whitelist:
            if w in data:
                data = data.replace(w, w.upper())
        data = data.replace(b, '')
    for w in whitelist:
        data = data.replace(w.upper(), w)
    data = data.strip()
    return data

# retrieves the 'num' table within the html
def getTableData(soup, num):
    data = []
    table = soup.findAll('table')
    table_body = table[num].find('tbody')

    rows = table_body.findAll('tr')
    for row in rows:
        cols = row.findAll('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele]) # Get rid of empty values
    for i in range(5):
        data[i][1] = blacklist(data[i][1])
    return data

# splits the data set with the hero and player
def splitHeroPlayer(data_in):
    data = {'hero' : '', 'player' : ''}
    for h in heroes:
        if h in data_in:
            data['hero'] = h
            data['player'] = data_in[:data_in.find(h)].strip()
            return data
    error("Hero not found")
    return 0

# checks data for val and return retval on success
def checkEmpty(data, retval):
    if data == '-':
        return retval
    else:
        return data

# returns a dictionary of data extracted from the overview page
def getOverviewData(soup):
    overview_data = {}
    # Get Radiant and Dire data
    data = getTableData(soup, 0) + getTableData(soup, 1)
    for i in range(10):
        hero_player = splitHeroPlayer(data[i][1])
        overview_data[hero_player['player']] = {'hero' : hero_player['hero'],
                                                'kills' : int(checkEmpty(data[i][2], 0)),
                                                'deaths' : int(checkEmpty(data[i][3], 0)),
                                                'wards' : int(checkEmpty(data[i][13][:data[i][13].find('/')], 0))}
    return overview_data
#
# Main
#

# Validate args
if(len(sys.argv) > 3):
    error("Too many arguments :: Format is python scrapper (url to team's game series) (number of series)")
    sys.exit()
if(len(sys.argv) < 2):
    error("Too little arguments :: Format is python scrapper (url to team's game series) (number of series)")
    sys.exit()
team_series_url = sys.argv[1]
if(len(sys.argv) <= 2):
    num_series = 20
else:
    num_series = sys.argv[2]
if not ('https://www.dotabuff.com/esports/teams/' in team_series_url):
    error('Malformed URL :: Must be https://www.dotabuff.com/esports/teams/...') 
    sys.exit()

# Connect to the Database
conn = sqlite3.connect("stats.db")
cur = conn.cursor()

# Soup and site connections
soup = getSoup(team_series_url)

# Obtain the selected series
series = getSeriesLinks(soup, num_series)

# Loop through every series and store the data
series = series[0:int(num_series)]
for series_link in series:
    # Check if this series is accounted for already

    # TODO :: Add SQLite lookup

    # Set up loop variables
    soup = getSoup(series_link)
    bo = getBestOf(soup)
    match_links = []
    v_idx = int(str(soup.title).find(' vs '))
    end_idx = int(str(soup.title).find(' - '))
    team_1 = str(soup.title)[7:v_idx]
    team_2 = str(soup.title)[v_idx+4:end_idx]
    team_1_wins = 0
    team_2_wins = 0

    match_links = getMatchLinks(soup)

    # Count every match up until a team wins 'bo' amount
    for match_link in match_links:
        soup = getSoup(match_link)

        # Find out who won
        v_idx = soup.get_text().find(' Victory!')
        team_1_idx = soup.get_text().find(team_1, v_idx-20, v_idx)
        team_2_idx = soup.get_text().find(team_2, v_idx-20, v_idx)
        if v_idx - team_1_idx < v_idx - team_2_idx:
            team_1_wins += 1
        else:
            team_2_wins += 1

        table = soup.findAll('table')
        #Extract all the neccesary data
        overview_data = getOverviewData(soup)
        soup = getSoup(match_link + '/combat')
        #combat_data = getCombatData(soup)
        #soup = getSoup(match_link + '/kills')
        #kills_data = getKillsData(soup)
        #soup = getSoup(match_link + '/objectives')
        #objectives_data = getObjectivesData(soup)
        #soup = getSoup(match_link + '/runes')
        #runes_data = getRunesData(soup)

    series_idx = series_link.find('/series/') + 8
    end_idx = series_link.find('-')
    log("Inserted data from series " + series_link[series_idx:end_idx] + " :: " + str(team_1_wins) + " - " + str(team_2_wins) + " :: " + team_1 + " - " + team_2)




