import os
from dotenv import load_dotenv
import requests
from urllib.parse import urlencode

# Load environment variables from .env file
load_dotenv()

def authenticate(email, password, clientid, secret):
    body_auth = {
        'username': email,
        'client_id': clientid,
        'client_secret': secret,
        'password': password,
        'grant_type': 'password',
        'platform': 'ipynb'
    }

    payload = urlencode(body_auth)
    url = 'https://api.xbrl.us/oauth2/token'
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    res = requests.post(url, data=payload, headers=headers)
    auth_json = res.json()

    if 'error' in auth_json:
        print("\n\nThere was a problem generating an access token with these credentials. Run the first cell again to enter credentials.")
        return None
    else:
        print("\n\nYour access token expires in 60 minutes.")
        return auth_json['access_token'], auth_json['refresh_token']
    
    
def get_access_token():
    email = os.getenv('XBRL_EMAIL')
    password = os.getenv('XBRL_PASSWORD')
    clientid = os.getenv('XBRL_CLIENT_ID')
    secret = os.getenv('XBRL_SECRET')
    
    if not all([email, password, clientid, secret]):
        print("Please ensure all XBRL environment variables are set.")
        return None
    
    # Call authenticate function and return the access token
    access_token, _ = authenticate(email, password, clientid, secret)
    return access_token
