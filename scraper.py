import bs4 as bs
import urllib.request
import requests
import sys
import sqlite3

#Format is 'python scrapper (url) (number of games)''

if(len(sys.argv) > 3):
	print("Too many arguments")

url = sys.argv[1]
numGames = sys.argv[2]

print(type(int(sys.argv[2])))

db = 'https://www.dotabuff.com'
hdr = { 'User-Agent' : 'fantasy stats' }
source = requests.get('https://www.dotabuff.com/esports/teams/2586976-og/series', headers=hdr)

soup = bs.BeautifulSoup(source.text, 'lxml')

#obtain the series
games = []

for url in soup.find_all('a'):
    if '/esports/series' in url.get('href'):
    	games.append(db + url.get('href'))

print(games)