###############################################
########### Get User Info for today ###########
###############################################

from twython import Twython
import time
import pandas as pd
import numpy as np
import datetime
import sqlite3
import pandas.io.sql as sql

# Twitter API authentication
app_key = ''
app_secret = ''
oauth_token = ''
oauth_token_secret = ''

twitter = Twython(app_key, app_secret, oauth_token, oauth_token_secret)

# Get all the Twitter handles we want to archive
f = open('UserAccounts.txt', 'r')
screen_names = [line.rstrip() for line in f.readlines()]

# Create empty list to store all user info retrieved by API
user_list = []

# Create empty list to temporarily store user info
user_info = []

# Iterate through list of screen_names
for idx, screen_name in enumerate(screen_names):
    
    print '======================================='
    print 'Requesting info For: ' + screen_name
    print str(idx+1) + ' of ' + str(len(screen_names)) + ' users'
    
    user_info = []

    # Request general user information
    resp = twitter.show_user(screen_name=screen_name)
    
    # Append fields to list
    user_info.append([resp['id'],
                    resp['screen_name'],
                    resp['name'],
                    resp['created_at'],
                    resp['followers_count'],
                    resp['friends_count'],
                    resp['statuses_count'],
                    resp['favourites_count'],
                    resp['listed_count']])
    
    # Add the list to user_list
    user_list.extend(user_info)    
    time.sleep(5)

# Create DataFrame from user_list
df = pd.DataFrame(user_list)

columns = ['user_id',
           'user_screen_name',
           'user_name',
           'user_created_at',
           'user_followers_count',
           'user_friends_count',
           'user_statuses_count',
           'user_favourites_count',
           'user_listed_count']

# Set columns
df.columns = columns

# Add a column with current datetime
df['api_request_datetime'] = datetime.datetime.today()

##################### CSV #####################

print '======================================='
print 'Saving to CSV'
print '======================================='

# Save as CSV
df.to_csv('CSV/UserInfoArchive/' + str(datetime.date.today()) + '_GetUserInfo.csv', encoding='utf-8', delimiter=',', index=False)

##################### SQL #####################

print 'Adding to SQL Database'
print '======================================='

# Fix date dtype so it can transfer to SQLite

df['api_request_datetime'] = df['api_request_datetime'].apply(str)

# Set connection string
con = sqlite3.connect("Twitter.db")
con.text_factory = str

# Write dataframe to SQLite
df.to_sql('Users', con, flavor='sqlite', if_exists='append', index=False)

print '===========REPORT=COMPLETE============='
print '======================================='