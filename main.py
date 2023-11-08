import pandas as pd
import requests
from bs4 import BeautifulSoup, SoupStrainer
from datetime import datetime as _datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as d2g
from gspread_dataframe import set_with_dataframe
import os
import urllib
import re
import json
import sys
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import base64


load_dotenv()

def data_to_sheets(data,ws,sheet):
    google_project_id    = os.environ['GOOGLE_PROJECT_ID']
    google_cred_password = os.environ['GOOGLE_CRED_PASSWORD']
    google_cred_username = os.environ['GOOGLE_CRED_USERNAME']
    google_client_id     = os.environ['GOOGLE_CLIENT_ID']
    google_key_id        = os.environ['GOOGLE_KEY_ID']

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

    print(data)

    print(ws)

    print(sheet)

    # d2g.upload(data, ws, sheet, credentials=credentials, row_names=False,clean=True)

    spreadsheet = gc.open_by_key(ws)
    worksheet   = spreadsheet.worksheet(sheet)

    set_with_dataframe(worksheet,data)

def explode_va(data):

    data['voteTotals'] = data['ballotOptions']['groupResults']

    return data

def get_va_data():
    link = 'https://enr.elections.virginia.gov/results/public/api/elections/Virginia/2023-Nov-Gen/files/json'

    _json = requests.get(link).text

    _data = json.loads(_json)['results']['ballotItems']
    
    data = pd.DataFrame(_data)

    data = data.explode('ballotOptions')

    data = data.apply(explode_va,axis=1)

    data = data.explode('voteTotals')

    data.rename({'id':'raceId','name':'fullRaceName'})
    data['raceId'] = data['id']
    data['raceType'] = data['name'].apply(lambda x: (re.search(r', .+ \(',x).group(0))[2:][:-2] if re.search(r', .+ \(',x) else np.nan)
    data['districtNumber'] = data['name'].apply(lambda x: re.search(r'\d+',x).group(0) if re.search(r'\d+',x) else np.nan)
    data['candidateId'] = data['ballotOptions'].apply(lambda x: x['id'])
    data['candidateName'] = data['ballotOptions'].apply(lambda x: x['name'])
    data['candidateBallotOrder'] = data['ballotOptions'].apply(lambda x: x['ballotOrder'])
    data['politicalParty'] = data['ballotOptions'].apply(lambda x: x['politicalParty'])
    # data['totalVoteCount'] = data['ballotOptions'].apply(lambda x: x['totalVoteCount'])

    data['voteType'] = data['voteTotals'].apply(lambda x: x['groupName'])
    data['voteCount'] = data['voteTotals'].apply(lambda x: x['voteCount'])
    data['isFromVirtualPrecinct'] = data['voteTotals'].apply(lambda x: x['isFromVirtualPrecinct'])
    

    data.drop(columns=['ballotOrder','ballotOptions','rankedChoiceResults','voteTotals'],inplace=True)


    data_to_sheets(data,'1GVrMS9gpoGQScWtRPz_ZNiWp0DL2rskraR7O4OkDXeU','data')



def get_data_from_container(state,updated_at,race_type,race_subtype,c):
    data = []
    race_name = None
    reporting = None

    updated_at = re.search(r'\| .+',updated_at).group(0)
    updated_at = updated_at.replace('| ','')

    if (len(c.select('.result-table-header'))>0) : race_name = c.select('.result-table-header')[0].get_text()
    if (len(c.select('.result-table-subheader'))>0) : reporting = c.select('.result-table-subheader')[0].get_text()

    for row in c.select('table.result-table>tbody>tr'):
        raw_candidate = None
        name = None
        party = None
        incumbent = None
        winner = None
        votes = None
        percent= None

        for i,td in enumerate(row.select("td")):
            _text = td.get_text()
            if i == 0:
                raw_candidate = _text
                name = re.sub(r' \(.+\).*','',_text)
                name = name.replace('\n',' ')

                if party := re.search(r'\(.+\)',_text):
                    party = party.group(0)
                    party = re.sub(r'[\(\)]','',party)

                if incumbent := re.search(r'\*',_text):
                    incumbent = incumbent.group(0)
                winner = 'winner' in td['class']
            elif i == 1:
                votes = _text
                if votes == '-' or votes == 'Unopposed':
                    votes = '0'
            elif i == 2:
                percent = _text

        _data = {
            "state":state,
            "updated_at":updated_at,
            "race_type":race_type,
            "race_subtype":race_subtype,
            "race_name":race_name,
            "reporting":reporting,
            "raw_candidate":raw_candidate,
            "candidate":name,
            "party":party,
            "votes":int(votes.replace(',', '')),
            "percent":percent,
            "incumbent":incumbent,
            "winner":winner
        }

        data.append(_data)

    return data


def get_county_level_data(event,context):
    print("-----TEST-----")

    # event = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    print(f"***STARTING COUNTY SCRAPE FOR {event['state']}***")

    start = _datetime.now()

    urls = [{'type':'Other Election Results','link':f"https://www.jsonline.com/elections/results/2022-11-08/state/{event['state']}"},
            {'type':'U.S. House','link':f"https://www.jsonline.com/elections/results/2022-11-08/us-house/{event['state']}"},
            {'type':'Illinois State Senate','link':f"https://www.jsonline.com/elections/results/2022-11-08/state/{event['state']}/upper"},
            {'type':'Illinois State House','link':f"https://www.jsonline.com/elections/results/2022-11-08/state/{event['state']}/lower"}
            ]

    data = []

    county_links = []
    for _url in urls:
        page = requests.get(_url['link'])
        soup = BeautifulSoup(page.text,features="lxml")

        for l in soup.select('.result-table-linkout-link'):
            if l.get_text() == '''County-by-County\nResults''':
                county_links.append({'type':_url['type'],'link':l['href']})

    race_start_time = _datetime.now()

    for l in county_links:
        race_start_time = _datetime.now()

        page = requests.get(l['link'])
        soup = BeautifulSoup(page.text,features="lxml")

        race = soup.select('.result-table-header')[0].get_text()
        updated_at = soup.select(".elections-header-subtitle")[0].get_text()

        for f in soup.select('.results-fips-container'):
            for c in f.select('.result-table-block'):
                data.append(get_data_from_container(event['state'],updated_at,l['type'],race,c))

        # print(f"{race} ended in {_datetime.now() - race_start_time}")


    data = [item for sublist in data for item in sublist]

    df = pd.DataFrame(data)

    if 'filter' in event.keys():
        ind = [True] * len(df)

        for col, vals in event['filter'].items():
            ind = ind & (df[col].isin(vals))

        df = df[ind]

    data_to_sheets(df,event['ws'],event['sheet'])

    print(f"FINISHED IN {_datetime.now() - start}")



def get_data(start,states):
    races = ['us-house','upper','lower']
    data = []
    base_url = 'https://www.jsonline.com/elections/results/2022-11-08/'

    state_start_time = _datetime.now()

    for state in states:
        state_start_time = _datetime.now()

        _url = base_url+"state/"+state

        page = requests.get(_url)
        soup = BeautifulSoup(page.text,features="lxml")
        updated_at = soup.select(".elections-header-subtitle")[0].get_text()

        for c in soup.select('.oxygen-package-body>.result-table-block'):
            data.append(get_data_from_container(state,updated_at,None,None,c))

        for f in soup.select('.results-fips-container'):
            if len(f.select(".result-table-linkout")) > 1:

                for c in f.select('.result-table-block'):
                    data.append(get_data_from_container(state,updated_at,f.select('.results-fips-title')[0].get_text(),None,c))

        for race in races:
            if race == 'us-house':
                _url = base_url+race+"/"+state
            else:
                _url = base_url+'state/'+state+'/'+race

            page = requests.get(_url)
            soup = BeautifulSoup(page.text,features="lxml")

            updated_at = soup.select(".elections-header-subtitle")[0].get_text()

            for f in soup.select('.results-fips-container'):
                for c in f.select('.result-table-block'):
                    data.append(get_data_from_container(state,updated_at,f.select('.results-fips-title')[0].get_text(),None,c))

        print(f"{state} ended in {_datetime.now() - state_start_time}")


    data = [item for sublist in data for item in sublist]

    df = pd.DataFrame(data)

    return df

def scrape(event,context):
    start = _datetime.now()

    print("***STARTING SCRAPE***")

    # event = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__)))

    f = open(os.path.join(__location__, event['states']))

    states = json.load(f)
    states = states['states']

    data = get_data(start,states)
    data_to_sheets(data,event['ws'],event['sheet'])

    print(f"FINISHED IN {_datetime.now() - start}")


if __name__ == "__main__":
    # get_county_level_data({'state':'illinois','ws':'1w644pHazhJg0YC1Io5YuAhWjnKgI8cevrmRE67CvCiE','sheet':'county_data'},'')
    # get_county_level_data({'state':'illinois','ws':'1vk9ZXXX8_u07crucrXosilsKV-NXAqhClVM0CWnrSEk','sheet':'raw_data','filter':{'race_type':['U.S. House'],'race_subtype':['District 14']}},'')
    # scrape({'states':'all_states.json','ws':'1My5BqqbIzysbXysOlZ2pcf0SoUHYBK7BGFqfTVen88A','sheet':'national_raw_data'},'')
    get_va_data()
    # scrape({'states':'arena_states.json','ws':'1My5BqqbIzysbXysOlZ2pcf0SoUHYBK7BGFqfTVen88A','sheet':'arena_raw_data'},'')
    # args = sys.argv
    # globals()[args[1]](*args[2:])
