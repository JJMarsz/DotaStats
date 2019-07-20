import bs4 as bs
import urllib.request
import requests
import sys
import sqlite3


def error(msg):
	print('[ERROR] :: ' + msg)

# Validate number of args
if(len(sys.argv) > 3):
    error("Too many arguments :: Format is python scrapper (url to team's game series) (number of games)")
    sys.exit()
if(len(sys.argv) < 2):
    error("Too little arguments :: Format is python scrapper (url to team's game series) (number of games)")
    sys.exit()

# Validate args
team_series_url = sys.argv[1]

if(len(sys.argv) <= 2):
	num_games = 20
else:
	num_games = sys.argv[2]

if not ('https://www.dotabuff.com/esports/teams/' in team_series_url):
    error('Malformed URL :: Must be https://www.dotabuff.com/esports/teams/...') 
    sys.exit()

db = 'https://www.dotabuff.com'
hdr = { 'User-Agent' : 'fantasy stats' }
source = requests.get(team_series_url, headers=hdr)

soup = bs.BeautifulSoup(source.text, 'lxml')

#obtain the series
games = []
next_page = '2'
next_page_link = ''
page_soup = soup
while len(games) < int(num_games):
    for url in page_soup.find_all('a'):
        if '/esports/series/' in url.get('href'):
            games.append(db + url.get('href'))
        if 'page=' + next_page == url.get('href')[len(url.get('href'))-6:len(url.get('href'))]:
            next_page_link = db + url.get('href')
    #set up the variables for the next page
    next_page_link = requests.get(next_page_link, headers=hdr)
    page_soup = bs.BeautifulSoup(next_page_link.text, 'lxml')
    next_page = str(int(next_page) + 1)

games = games[0:int(num_games)]
for series_link in games:
    print(series_link)