import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime as _datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
import os


url={
'state':'texas',
'race':'senate'
}
base_url = 'https://www.politico.com/2020-election/results/'

_url = base_url+url['state']+'/'+url['race']
print(_url)
page = requests.get(_url)
soup = BeautifulSoup(page.text,features='lxml')

containers = soup.select('div.smaller-leaderboard-container,div.leaderboard-holder-child.primary-column')

for c in containers:
    print(c)
    id = c.find_next('div').get('id')
    rows = c.find('table',class_='candidate-table').find('tbody').find_all('tr')

    for r in rows:
        _data = {
            'state':url['state'],
            'office':url['race'],
            'id':id,
            'candidate':r.find('div',class_='candidate-short-name').get_text(),
            'party':r.find('div',class_='party-label').get_text(),
            'votes':r.find('div',class_='candidate-votes-next-to-percent').get_text(),
            'percent':r.find('div',class_='candidate-percent-only').get_text(),
            'precincts':c.find('div','vote-progress').get_text(),
            'called':c.find('div',class_='candidate-winner-check').get_text()
        }

        print(_data)
