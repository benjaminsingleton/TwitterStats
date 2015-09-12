import pandas as pd
import numpy as np
import datetime
import sqlite3
import pandas.io.sql as sql
from dateutil.parser import parse
pd.set_option('display.max_rows', 500)


# Enter the start and end date. You may have to re-enter start and end in followers section if followers
# were not collected on the start and end dates.
start_date = raw_input('Enter a start date: ')
end_date = raw_input('Enter a end date: ')

start_date_raw = pd.to_datetime(parse(str(start_date)))
start_date = str(start_date_raw.date())
end_date_raw = pd.to_datetime(parse(str(end_date)))
end_date = str(end_date_raw.date())

evaluation_period = end_date_raw - start_date_raw
evaluation_period = evaluation_period.days + 1
print 'Evaluation period: ' + str(evaluation_period) + ' days'
print

# Set connection string
con = sqlite3.connect("Twitter.db")
con.text_factory = str

# Query SQLite to get tweets
sql = ("SELECT * FROM Tweets")

# Add query results to df 
df = pd.read_sql(sql, con)

df = df.drop_duplicates('tweet_id')


# Sort by tweet id then date descending
df = df.sort(['tweet_id', 'api_request_datetime'], ascending=[1, 0])


def parse_date(x):
	try:
		return parse(x)
	except ValueError:
		return pd.NaT

df.index = pd.to_datetime(df['tweet_created_at'].map(lambda x: parse_date(x)))

# Force 'tweet_is_a_retweet' to Boolean
d = {'True': True, 'False': False, 1:True, 0:False}
df['tweet_is_a_retweet'] = df['tweet_is_a_retweet'].map(d)


# Filter by start and end date
df = df.ix[start_date:end_date]


# Get all the Twitter handles we want to analyze
f = open('UserAccounts.txt', 'r')
screen_names = [line.rstrip() for line in f.readlines()]

# Filter by screen names in case there is bad data
df = df[df['user_screen_name'].isin(screen_names)]


# Total tweets for all accounts

total_tweets = df.ix[start_date:end_date]['tweet_id'].count()
print 'Total tweets:          ', df.ix[start_date:end_date]['tweet_id'].count()

original_tweets = df[df['tweet_is_a_retweet'] == False].ix[start_date:end_date]['tweet_id'].count()
print 'Total original tweets: ', original_tweets
print 'Original tweet %:      ', round(float(original_tweets)/total_tweets, 2)

retweeted_tweets = df[df['tweet_is_a_retweet'] == True].ix[start_date:end_date]['tweet_id'].count()
print 'Total retweeted tweets:', retweeted_tweets
print 'Retweeted tweet %:     ', round(float(retweeted_tweets)/total_tweets, 2)
print

# Total retweets for all accounts

total_original_retweets = df[df['tweet_is_a_retweet'] == False].ix[start_date:end_date]['tweet_retweet_count'].sum()

print 'Total retweets across all accounts: ' + str(int(total_original_retweets))


# Total favorites for all accounts

total_original_favorites = df[df['tweet_is_a_retweet'] == False].ix[start_date:end_date]['tweet_favorite_count'].sum() 

print 'Total favorites across all acounts: ' + str(total_original_favorites)
print

tweet_count = df.groupby('user_screen_name')['tweet_id'].count().order()

print '------------Tweet Count------------'
print tweet_count
print '-----------------------------------'

tweet_rate = tweet_count/evaluation_period
print '------------Tweets per Day------------'
print tweet_rate
print '--------------------------------------'

print '------------Tweet Percentiles------------'
print 'Min            ', np.amin(tweet_rate)
print '25th Percentile', np.percentile(tweet_rate, 25)
print '50th Percentile', np.percentile(tweet_rate, 50)
print '75th Percentile', np.percentile(tweet_rate, 75)
print 'Max            ', np.amax(tweet_rate)
print '-----------------------------------------'

print '------------ 25th Percentile and below ------------'
print tweet_rate[tweet_rate<=np.percentile(tweet_rate, 25)]
print '----------------------------------------------'

print '------------ 25th to 50th Percentile------------'
print tweet_rate[(tweet_rate>np.percentile(tweet_rate, 25)) & (tweet_rate<=np.percentile(tweet_rate, 50))]
print '---------------------------------------------------'

print '------------ 50th to 75th Percentile ------------'
print tweet_rate[(tweet_rate>np.percentile(tweet_rate, 50)) & (tweet_rate<=np.percentile(tweet_rate, 75))]
print '-----------------------------------'

print '------------ Greater than 75th Percentile ------------'
print tweet_rate[tweet_rate>np.percentile(tweet_rate, 75)]
print '--------------------------------------------------'

# Limit dataframe to only original tweets
df_original = df[df['tweet_is_a_retweet']==False]


# Get original tweet percentage by account
print '----------Original Tweet Percentage ----------'
original_tweets_count = df_original.groupby('user_screen_name')['tweet_id'].count().order()
orig_tweet_perc = original_tweets_count / tweet_count
orig_tweet_perc.sort(ascending=False)
print orig_tweet_perc
print '----------------------------------------------'


# Get top 10 tweets by retweet count

df_top_tweets = df_original.sort(['tweet_retweet_count'], ascending=0).head(10)

print '----------Top 10 Retweeted Tweets ----------'


for count, row in df_top_tweets.iterrows():
    
    print '-------------------------'
    print 
    print 'Tweet date:      ' + str(df['tweet_created_at'][count])
    print 'Tweet account:   ' + str(df['user_screen_name'][count])
    print 'Tweet:           ' + str(df['tweet_text'][count])
    print 'Tweet Retweets:  ' + str(df['tweet_retweet_count'][count])
    print 'Tweet Favorites: ' + str(df['tweet_favorite_count'][count])
    print
print '----------------------------------------------'


####### FOLLOWERS ##########

# Query SQLite to get tweets
sql = ("SELECT * FROM Users")

# Add query results to df 
df = pd.read_sql(sql, con)

df['api_request_datetime'] = pd.to_datetime(df['api_request_datetime'])

df.index = df['api_request_datetime']

# Filter by screen names in case there is bad data
df = df[df['user_screen_name'].isin(screen_names)]


# Get unique api_request_datetimes values
unique_dates = df['api_request_datetime'].apply(lambda x: x.date()).unique()
unique_dates = unique_dates.tolist()

# Get closest start_date because we may not have indexed on exact date
def func(x):
    delta =  x - parse(end_date).date()
    return abs(delta)
 
closest_end_date = str(min(unique_dates, key = func))


# Get closest end_date because we may not have indexed on exact date
def func(x):
    delta =  x - parse(start_date).date()
    return abs(delta)
 
closest_start_date = str(min(unique_dates, key = func))

print 'The closest start date with follower data is: ' + closest_start_date
print 'The closest end date with follower data is:   ' + closest_end_date
print

# Followers

start_followers = float(df.ix[closest_start_date].drop_duplicates('user_screen_name')['user_followers_count'].sum())
end_followers = float(df.ix[closest_end_date].drop_duplicates('user_screen_name')['user_followers_count'].sum())
print 'Followers on ' + closest_start_date + ': ' + str(int(start_followers))
print 'Followers on ' + closest_end_date + ': ' + str(int(end_followers))
print

# Change in followers

print 'The followers changed by ' + str(round((end_followers - start_followers)/start_followers, 3)*100) + ' percent.'
print

df = df.ix[closest_end_date].drop_duplicates('user_screen_name')

follower_count = df.groupby('user_screen_name')['user_followers_count'].sum().order(ascending=False)

print '--------Follower Count on ' + closest_end_date + '------------'
print follower_count
print '--------------------------------------'
print
print 'Total followers not including NYPDnews and Comm. Bratton: ' + str(df['user_followers_count'].sum())
print

print '------------Follower Percentiles------------'
print 'Min            ', np.amin(follower_count)
print '25th Percentile', np.percentile(follower_count, 25)
print '50th Percentile', np.percentile(follower_count, 50)
print '75th Percentile', np.percentile(follower_count, 75)
print 'Max            ', np.amax(follower_count)
print '-----------------------------------------'

print '---------- 1st to 25th Percentile ------------'
print follower_count[follower_count<=np.percentile(follower_count, 25)].order()
print '----------------------------------------------'

print '------------ 25th to 50th Percentile------------'
print follower_count[(follower_count>np.percentile(follower_count, 25)) & (follower_count<=np.percentile(follower_count, 50))].order()
print '---------------------------------------------------'

print '------------ 50th to 75th Percentile------------'
print follower_count[(follower_count>np.percentile(follower_count, 50)) & (follower_count<=np.percentile(follower_count, 75))].order()
print '---------------------------------------------------'

print '------------ Above 75th Percentile ------------'
print follower_count[follower_count>np.percentile(follower_count, 75)].order()
print '--------------------------------------------------'

print 'END OF REPORT'
