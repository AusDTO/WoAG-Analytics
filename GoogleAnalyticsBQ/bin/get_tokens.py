#!/usr/bin/python

import os
import sys

EGG_DIR = "/home/mark/"

for filename in os.listdir(EGG_DIR):
    if filename.endswith(".egg"):
        sys.path.append(EGG_DIR + filename) 

# Credentials you get from registering a new application
#client_id = '<the id you get from google>.apps.googleusercontent.com'
client_id = raw_input('Your client ID:')
#client_secret = '<the secret you get from google>'
client_secret = raw_input('Your client secret:')
redirect_uri = 'urn:ietf:wg:oauth:2.0:oob'

# OAuth endpoints given in the Google API documentation
authorization_base_url = "https://accounts.google.com/o/oauth2/auth"
token_url = "https://accounts.google.com/o/oauth2/token"
scope = [ "https://www.googleapis.com/auth/bigquery.readonly" ]

from requests_oauthlib import OAuth2Session
google = OAuth2Session(client_id, scope=scope, redirect_uri=redirect_uri)

# Redirect user to Google for authorization
authorization_url, state = google.authorization_url(authorization_base_url, access_type="offline", approval_prompt="force")
    # offline for refresh token
    # force to always make user click authorize
print 'Please go here and authorize: ', authorization_url

# Get the authorization verifier code from the callback url
redirect_response = raw_input('Paste the full authorization code here:')

# Fetch the access token
t = google.fetch_token(token_url, client_secret=client_secret, code=redirect_response)

print "Token details...\nAccess token: %s\nRefresh token: %s\n" % ( t['access_token'], t['refresh_token'])
