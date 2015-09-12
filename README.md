# TwitterStats
### Tweet and follower analysis for any number of Twitter accounts.

TwitterStats was one of my first projects using Python. The goal was to retrieve data via the Twitter API for any number of TwitterAccounts and generate a report about tweet & follower activity. The project was written for the NYPD Twitter pilot.

### How to use it

1. Edit the `UserAccounts.txt` file by adding any number of Twitter accounts you want to index. Each account needs to be on its own line and each account should match how it appears on Twitter (it's case sensitivie, e.g. BenSingletonNYC not bensingletonnyc).

2. In command line, type `python GetUserTimeline.py` to retrieve all tweets for the user accounts listed in `UserAccounts.txt`. Make sure to enter your Twitter API authentication credentials first.

3. In command line, type `python GetUserInfo.py` to retreive follower counts for the user accounts listed in `UserAccounts.txt`. Like above, make sure to enter your Twitter API authentication credentials first.

4. In command line, type `python TwitterReport.py` to get a number of overview reporting statistics. Enter a start and end date when prompted.

