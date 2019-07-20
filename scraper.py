import bs4 as bs
import urllib.request
import requests
import sys
import sqlite3


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()

def error(msg):
    print('[ERROR] :: ' + msg)

# Validate number of args
if(len(sys.argv) > 3):
    error("Too many arguments :: Format is python scrapper (url to team's game series) (number of series)")
    sys.exit()
if(len(sys.argv) < 2):
    error("Too little arguments :: Format is python scrapper (url to team's game series) (number of series)")
    sys.exit()

# Validate args
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

# Some useful constants
db = 'https://www.dotabuff.com'
hdr = { 'User-Agent' : 'fantasy stats' }

# Soup and site connections
source = requests.get(team_series_url, headers=hdr)
soup = bs.BeautifulSoup(source.text, 'lxml')

# Obtain the selected series
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
    next_page_link = requests.get(next_page_link, headers=hdr)
    soup = bs.BeautifulSoup(next_page_link.text, 'lxml')
    next_page = str(int(next_page) + 1)

# Loop through every series and store the data
series = series[0:int(num_series)]
for series_link in series:
    # Check if this series is account for already

    # TODO :: Add SQLite lookup

    # Set up loop variables
    source = requests.get(series_link, headers=hdr)
    soup = bs.BeautifulSoup(source.text, 'lxml')
    bo = -1
    match_links = []
    v_idx = int(str(soup.title).find(' vs '))
    end_idx = int(str(soup.title).find(' - '))
    team_1 = str(soup.title)[7:v_idx]
    team_2 = str(soup.title)[v_idx+4:end_idx]
    team_1_wins = 0
    team_2_wins = 0
    # Find the type of series
    if 'Best of ' in soup.get_text():
        idx = soup.get_text().find('Best of ')
        bo = int(soup.get_text()[idx + 8])
    else:
        error('Cannot find the amount of games in the series')

    # Get every match link in the page
    for url in soup.find_all('a'):
        if '/matches/' in url.get('href'):
            match_links.append(db + url.get('href'))

    # Count every match up until a team wins 'bo' amount
    for match_link in match_links:
        if team_1_wins == int(bo/2)+1 or team_2_wins == int(bo/2)+1 or (bo == 2 and team_1_wins + team_2_wins == bo):
            break
        # Set up loop variables
        source = requests.get(match_link, headers=hdr)
        soup = bs.BeautifulSoup(source.text, 'lxml')

        # Find out who won
        idx_1 = soup.get_text().find(team_1 + " Victory!")
        if idx_1 != -1:
        	team_1_wins += 1
        else:
        	team_2_wins += 1
    series_idx = series_link.find('/series/') + 8
    end_idx = series_link.find('-')
    print("Inserted data from series " + series_link[series_idx:end_idx] + " :: " + str(team_1_wins) + " - " + str(team_2_wins) + " :: " + team_1 + " - " + team_2)




