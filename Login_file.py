import time
import urllib.parse as urlparse
import requests
from fyers_api import fyersModel
from fyers_api import accessToken
import traceback
import json
import pandas as pd



def get_token():

    # There has to be a pickle file named credentials that has all the account data.
    #df = pd.read_pickle('credentials.pkl')
    # FILL YOUr DETAILS from a pickle file which has all this data
    app_id='SXHBJ2V13T-100'
    app_id_post = app_id.split('-')[0]
    app_secret='THCKR2YIMC'
    redirect_url='https://127.0.0.1'
    client_id='XP00637'
    password='India@2021'
    two_fa='AEYPM9611H'
    try:
        appSession = accessToken.SessionModel(client_id=app_id,
                                              secret_key=app_secret,
                                              redirect_uri=redirect_url,
                                              response_type='code',
                                              grant_type='authorization_code',
                                              state='temp',
                                              nonce='private')
        response_url = appSession.generate_authcode()
        print(response_url)

        headers = {
            "accept": "*/*",
            "accept-language": "en-IN,en-US;q=0.9,en;q=0.8",
            "content-type": "application/json; charset=UTF-8",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "referrer": response_url
        }
        input = {"fyers_id": client_id, "password": password, "pan_dob": two_fa, "app_id": app_id_post,
         "redirect_uri": redirect_url, "appType": "100", "code_challenge": "", "state": "private",
         "scope": "", "nonce": "private", "response_type": "code", "create_cookie": True}

        session = requests.Session()

        result = session.post('https://api.fyers.in/api/v2/token', headers=headers, json=input, allow_redirects=True)

        var = json.loads(result.content)
        URL = var["Url"]
        print(URL)
        parsed = urlparse.urlparse(URL)
        parsedlist = urlparse.parse_qs(parsed.query)['auth_code']
        auth_code = parsedlist[0]
        appSession.set_token(auth_code)
        response = appSession.generate_token()

        access_token = response["access_token"]
        print(access_token)
        # Write the access token into a file here to read it in the main execution program
        file = open("fyersToken.txt","w")
        file.write(access_token)
        file.close()
        return access_token, app_id

    except Exception as e:
        print(e)
        return 'error'

def set_token_requests():

    try:
        access_token, app_id = get_token()
    except:
        print('error')
        return


    fyers = fyersModel.FyersModel(client_id=app_id,
                                   token=access_token,
                                   log_path='C:\\Users\\geeta\\Desktop\\Execution\\BOS')
    print(fyers.get_profile())

# Run this function to store the access token in text file
set_token_requests()
