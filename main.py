import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime as _datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
import os
import urllib

def scrape(event,context):
    start = _datetime.now()

    google_project_id    = os.environ['google_project_id']
    google_cred_password = os.environ['google_cred_password']
    google_cred_username = os.environ['google_cred_username']
    google_client_id     = os.environ['google_client_id']
    google_key_id        = os.environ['google_key_id']

    _cred = {
        "type": "service_account",
        "private_key_id":google_key_id,
        "project_id": google_project_id,
        "private_key": google_cred_password.replace('\\n', '\n'),
        "client_email": google_cred_username,
        "client_id":google_client_id,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/"+urllib.parse.quote(google_cred_username)
    }

    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(_cred, scope)
    gc = gspread.authorize(credentials)

    states = ['alabama',
            'alaska',
            'arizona',
            'arkansas',
            'california',
            'colorado',
            'connecticut',
            'delaware',
            'florida',
            'georgia',
            'hawaii',
            'idaho',
            'illinois',
            'indiana',
            'iowa',
            'kansas',
            'kentucky',
            'louisiana',
            'maine',
            'maryland',
            'massachusetts',
            'michigan',
            'minnesota',
            'mississippi',
            'missouri',
            'montana',
            'nebraska',
            'nevada',
            'new-hampshire',
            'new-jersey',
            'new-mexico',
            'new-york',
            'north-carolina',
            'north-dakota',
            'ohio',
            'oklahoma',
            'oregon',
            'pennsylvania',
            'rhode-island',
            'south-carolina',
            'south-dakota',
            'tennessee',
            'texas',
            'utah',
            'vermont',
            'virginia',
            'washington',
            'washington-dc',
            'west-virginia',
            'wisconsin',
            'wyoming'
]

    races = ['house','senate','governor']

    base_url = 'https://www.politico.com/2020-election/results/'

    data = []

    for url in [({'state':x,'race':y}) for x in states for y in races]:
        _url = base_url+url['state']+'/'+url['race']
        try:
            page = requests.get(_url)
            soup = BeautifulSoup(page.text,features='lxml')

            print(_url)

            containers = soup.select('div.smaller-leaderboard-container,div.leaderboard-holder-child.primary-column')

            for c in containers:
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

                    data.append(_data)

        except:
            pass

    final_data = pd.DataFrame(data)

    print(final_data.columns)

    index_races = final_data[(final_data['office']=='house') & (final_data['id'].isnull())].index
    final_data.drop(index_races,inplace=True)

    d2g.upload(final_data, '10_bhKbRqrywCer0flwXO9UPxdCspOnvfD8ESP9ZV45M', 'raw_data', credentials=credentials, row_names=False,clean=True)

    print(_datetime.now() - start)

if __name__ == "__main__":
    scrape('','')
