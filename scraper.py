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
    # Set up loop variables
    source = requests.get(series_link, headers=hdr)
    soup = bs.BeautifulSoup(source.text, 'lxml')
    bo = -1
    # Find all the games
    if 'Best of ' in soup.get_text():
        idx = soup.get_text().find('Best of ')
        bo = int(soup.get_text()[idx + 8])
    else:
    	error('Cannot find the amount of games in the series')
    print(bo)
    for url in soup.find_all('a'):
        if 'matches/' in url.get('href'):
            print(url.get('href'))

