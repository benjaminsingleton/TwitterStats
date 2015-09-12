###############################################
# Get User Timeline for the previous two days #
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

# Get time to calculate time to run script at end
datetime_today = datetime.datetime.today()

# List of all the Twitter handles we want to archive
f = open('UserAccounts.txt', 'r')
screen_names = [line.rstrip() for line in f.readlines()]

# Set variable to current datetime to timestamp records
api_request_datetime = pd.to_datetime(datetime.datetime.today())

# Set connection string
con = sqlite3.connect("Twitter.db")
con.text_factory = str

# Create empty list to store all tweets retrieved by API
tweets = []

# Iterate through list of screen_names
for idx, screen_name in enumerate(screen_names):
    
    print '======================================='
    print 'Requesting Tweets For: ' + screen_name
    print str(idx+1) + ' of ' + str(len(screen_names)) + ' users'
    
    tweet_count = 0 

    # Query SQLite filtered by username for last tweet ID
    
    sql = ("SELECT * FROM Tweets WHERE user_screen_name LIKE \'" + screen_name + "\' ORDER BY tweet_created_at DESC LIMIT 3")

    since_id = 0
    id_date = 0
    
    try:
        since_id = pd.read_sql(sql, con)['tweet_id'][2]
        id_date = pd.read_sql(sql, con)['tweet_created_at'][2]
    except:
        since_id = 0
        id_date = 0
        pass

    print 'Since ID: ' + str(since_id) + ', Date: ' + str(id_date)
    
    if since_id > 0:
        resp = twitter.get_user_timeline(screen_name=screen_name,count=200, since_id=str(since_id))
    else:
        print 'No archived tweets'
        resp = twitter.get_user_timeline(screen_name=screen_name,count=200)

    tweets.extend(resp)
    tweet_count = tweet_count + len(resp)

    # Sleep 5 seconds to comply with API rate limiting
    time.sleep(5)

    print 'Tweets Collected: ' + str(tweet_count)

    while len(resp) == 200:
      oldest_id = resp[-1]['id']

      if oldest_id > since_id:
        print 'Looking for more tweets ...'
        created_at = resp[-1]['created_at']
        resp = []

        print 'Before ID: ' + str(oldest_id) + ', Date: ' + str(created_at)

        # Get older tweets
        resp = twitter.get_user_timeline(screen_name=screen_name,count=200,max_id=str(oldest_id))

        tweets.extend(resp)

        tweet_count = tweet_count + len(resp)

        # Sleep 5 seconds to comply with API rate limiting
        time.sleep(5)
        
        print 'Tweets Collected: ' + str(tweet_count)

      else: resp = []
    
    print 'Total Tweets Retrieved: ' + str(len(tweets))
    
print '======================================='
print 'Total Tweets Retrieved: ' + str(len(tweets))
print '======================================='

# Create DataFrame from tweets list object generated from API
df = pd.DataFrame(tweets)

# Set the right column names
if len(df.columns.values.tolist()) == 26:

    # Set more descriptive column names
    df.columns=['tweet_contributors', 
                'tweet_coordinates', 
                'tweet_created_at', 
                'tweet_entities', 
                'tweet_extended_entities',
                'tweet_favorite_count', 
                'tweet_favorited', 
                'tweet_geo', 
                'tweet_id', 
                'tweet_id_str', 
                'tweet_in_reply_to_screen_name',
                'tweet_in_reply_to_status_id',
                'tweet_in_reply_to_status_id_str', 
                'tweet_in_reply_to_user_id', 
                'tweet_in_reply_to_user_id_str',
                'tweet_is_quote_status',
                'tweet_lang', 
                'tweet_place', 
                'tweet_possibly_sensitive',
                'tweet_retweet_count', 
                'tweet_retweeted', 
                'tweet_retweeted_status',
                'tweet_source', 
                'tweet_text', 
                'tweet_truncated', 
                'tweet_user']

elif len(df.columns.values.tolist()) == 29:

    # Set more descriptive column names
    df.columns=['tweet_contributors', 
                'tweet_coordinates', 
                'tweet_created_at', 
                'tweet_entities', 
                'tweet_extended_entities',
                'tweet_favorite_count', 
                'tweet_favorited', 
                'tweet_geo', 
                'tweet_id', 
                'tweet_id_str', 
                'tweet_in_reply_to_screen_name',
                'tweet_in_reply_to_status_id',
                'tweet_in_reply_to_status_id_str', 
                'tweet_in_reply_to_user_id', 
                'tweet_in_reply_to_user_id_str',
                'tweet_is_quote_status',
                'tweet_lang', 
                'tweet_place', 
                'tweet_possibly_sensitive',
                'quoted_status', 
                'quoted_status_id', 
                'quoted_status_id_str',
                'tweet_retweet_count', 
                'tweet_retweeted', 
                'tweet_retweeted_status',
                'tweet_source', 
                'tweet_text', 
                'tweet_truncated', 
                'tweet_user']

# Convert to pandas datetime Series
df['tweet_created_at'] = pd.to_datetime(df['tweet_created_at'])

# Add a column with boolean values if there is a value for 'tweet_retweet_status'
df['tweet_is_a_retweet'] = df['tweet_retweeted_status'].notnull()

# Add a column with the datetime of the API request
df['api_request_datetime'] = api_request_datetime

# Create empty lists to store user data
user_list = []

print 'Parsing through the API table'
print '======================================='

# Iterate through the DataFrame, row by row, pulling out data we want to store
# and add to pertinent lists
for count, row in df.iterrows():
    tweet_info = []
    tweet_id = df['tweet_id'][count]
    
    # Flatten the nested tweet_user dict values 
    tweet_info = []
    
    # Append values we care about to tweet_info temporary list
    tweet_info.append([df['tweet_user'][count]['id'],
                      df['tweet_user'][count]['screen_name'],
                      df['tweet_user'][count]['name'],
                      df['tweet_user'][count]['followers_count'],
                      df['tweet_user'][count]['friends_count'],
                      df['tweet_user'][count]['statuses_count'],
                      df['tweet_user'][count]['favourites_count']])

    # Add the list to user_list
    user_list.extend(tweet_info)

print 'Creating DataFrame'
print '======================================='

# Create a DataFrame from user_list and set column names
df_user = pd.DataFrame(user_list, columns=(['user_id', 
                                            'user_screen_name', 
                                            'user_name',
                                            'user_followers_count',
                                            'user_friends_count',
                                            'user_statuses_count',
                                            'user_favorites_count']))

# Combine tweets DataFrame with user DataFrame (add df_user columns to the right of df)
df = pd.concat([df, df_user], axis=1)

print 'Formatting'
print '======================================='

# Delete unnecessary columns from tweets DataFrame
df = df.drop(['tweet_user'],axis=1)

# Reorder columns for main tweets DataFrame
columns = ['tweet_id',
           'tweet_created_at',
           'tweet_text',
           'tweet_favorite_count',
           'tweet_retweet_count',
           'tweet_is_a_retweet',
           'user_id',
           'user_screen_name',
           'user_name',
           'user_followers_count',
           'user_friends_count',
           'user_statuses_count',
           'user_favorites_count',
           'api_request_datetime']

# Set df_final to new reordering
df = df[columns]

##################### CSV #####################

print 'Saving to CSV'
print '======================================='

# Save df_final DataFrame to CSV
df.to_csv('CSV/TweetsArchive/' + str(datetime.date.today()) + '_TweetsByAccount.csv', encoding='utf-8', index=False)

##################### SQL #####################

print 'Adding to SQL Database'
print '======================================='

# Set connection string
con = sqlite3.connect("Twitter.db")
con.text_factory = str

# Read from CSV we just saved because columns have datatypes that SQLite will accept (i.e. no Nan, None, etc.)
df_final_sql = pd.read_csv('CSV/TweetsArchive/' + str(datetime.date.today()) + '_TweetsByAccount.csv', delimiter=',')

# Write dataframe to SQLite
df_final_sql.to_sql('Tweets', con, flavor='sqlite', if_exists='append', index=False)

# Current time
time_complete = datetime.datetime.today()

print '======================================='
print '==============Job Complete============='
print 'Time Elapsed: ' + str(time_complete-datetime_today) + '==========='