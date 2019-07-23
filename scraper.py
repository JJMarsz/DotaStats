import bs4 as bs
import urllib.request
import requests
import sys
import sqlite3

# Control flags
fail_error = 0
log_lvl = 0

# Some useful constants
db = 'https://www.dotabuff.com'
hdr = { 'User-Agent' : 'fantasy stats' }

#
# Subroutines
#
def error(msg):
    print('[ERROR] :: ' + msg)
    if fail_error:
        sys.exit()

# requests a soup of the url
def getSoup(url):
    source = requests.get(url, headers=hdr)
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
# returns a dictionary of data extracted from the overview page
def getOverviewData(soup):
    overview_data = {'kills' : {}, 'deaths' : {}, 'lh_and_d' : {}, 'gpm' : {}, 'wards' : {}, 'hero' : {}}
    players = []
    heroes = []
    # Get Radiant and Dire data
    data = getTableData(soup, 0) + getTableData(soup, 1)
    for i in range(10):
           print(data[i][1])
    players = players[0:10]
    heroes = heroes[0:10]
    #for i in range(5):
     #   overview_data['hero'][players[i]] = heroes[i]


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
    print("Inserted data from series " + series_link[series_idx:end_idx] + " :: " + str(team_1_wins) + " - " + str(team_2_wins) + " :: " + team_1 + " - " + team_2)




