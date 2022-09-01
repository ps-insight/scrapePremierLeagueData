import json
import aiohttp
import asyncio
import pandas as pd
import sqlalchemy
import requests
from understat import Understat
import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

# I read my database creds into a "dbcreds" JSON object and read it from there. 
database_username = dbcreds["mysql"]["user"]
database_password = dbcreds["mysql"]["password"]
database_ip       = dbcreds["mysql"]["host"]
database_name     = 'premierleague'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password,
                                                          database_ip, database_name))

# FPL Site Data
logger.info("Fetching FPL site data")
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
r = requests.get(url)
payload = r.json()
players_df = pd.DataFrame(payload['elements'])
teams_df = pd.DataFrame(payload['teams'])
position_df = pd.DataFrame(payload['element_types'])
players_df = players_df[["id", "element_type", "first_name", "second_name", "photo", "team", "web_name", "points_per_game", "now_cost", "clean_sheets"]]
players_df['player'] = players_df[['first_name', 'second_name']].agg(' '.join, axis=1)
players_df['position'] = players_df.element_type.map(position_df.set_index('id').plural_name_short)
players_df['team_name'] = players_df.team.map(teams_df.set_index('id').name)
players_df.drop(['element_type', 'team'], axis=1)
players_df['now_cost'] = players_df['now_cost'].apply(lambda x: x/10)
logger.info("Saving FPL site data")
players_df.to_sql(con=database_connection, name='players_fpl', if_exists='replace', index=False)

# Understat Data
logger.info("Fetching Understat site data")
async def fetchPlayersUnderstat():
    season = 2021
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        players = await understat.get_league_players("EPL", season)
    #Close the session
    await session.close()
    playersunderstat_df = pd.DataFrame(players)
    logger.info("Saving Understat site data")
    playersunderstat_df.to_sql(con=database_connection, name='players_understat', if_exists='replace', index=False)
loop = asyncio.get_event_loop()
loop.run_until_complete(fetchPlayersUnderstat())

# FBRef Data
logger.info("Fetching FBRef site data")
url = 'https://fbref.com/en/comps/9/stats/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)
df[0].columns = df[0].columns.droplevel(0) # Drop header level row
df[0] = df[0].drop(['# Pl','Age','Poss','MP','Starts','Min','90s','PKatt'], axis=1) # Drop unused columns
# Rename columns for DB
df[0].columns = ['Team','Goals','Assists', 'npGoals', 'Penalties', 'CardsYellow', 'CardsRed', 'Goals90', 'Assists90','GI90','npGoals90','npGI90','xG','npxG','xA','npxGI','xG90','xA90','xGI90','npxG90','npxGI90']
# Name dataframe
dfTeamStandardStats = df[0]
df[1].columns = df[1].columns.droplevel(0) # Drop header level row
df[1] = df[1].drop(['# Pl','Age','Poss','MP','Starts','Min','90s','PKatt','xA'], axis=1) # Drop unused columns
# Rename columns for DB
df[1].columns = ['Team','Goals','Assists', 'npGoals', 'Penalties', 'CardsYellow', 'CardsRed', 'Goals90', 'Assists90','GI90','npGoals90','npGIc90','xGc','npxGc','npxGIc','xGc90','xGIc90','npxGc90','npxGIc90']
dfTeamVsStandardStats = df[1]

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Born','Squad'], axis=1) # Drop unused columns
# Split duplicate columns due to "Per 90s" stats
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)
# Rename columns to clearify column/stat
df[2].rename(columns =
{
    'Gls': 'Goals',
    'Ast': 'Assists',
    'G-PK': 'npGoals',
    'PK': 'Penalties',
	'PKAtt':'PenaltyAttempts',
    'CrdY': 'CardsYellow',
    'CrdR': 'CardsRed',
    'Gls1': 'G90',
    'Ast1': 'A90',
    'G+A': 'GI90',
    'G+A-PK': 'npGI90',
    'G-PK1': 'npG90',
    'xG1': 'xG90',
    'xA1': 'xA90',
    'xG+xA': 'xGI90',
    'npxG1': 'npxG90',
    'npxG+xA1': 'npxGI90'
}, inplace = True)
# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')
# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)
# Name dataframe
dfPlayerStandardStats = df[2]

url = 'https://fbref.com/en/comps/9/misc/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)
df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Squad','Pos','Age','Born','90s','CrdY','CrdR','Crs','Int','TklW','2CrdY'], axis=1) # Drop unused columns
df[2].rename(columns = {
    'Fls': 'FoulsCommitted',
    'Fld': 'FoulsDrawn',
    'Off': 'Offsides',
    'PKwon': 'PenaltiesWon',
    'PKcon': 'PenaltiesConceded',
    'Recov': 'BallsRecovered',
    'OG': 'OwnGoals',
    'Won': 'AerialsWon',
    'Lost': 'AerialsLost',
    'Won%': 'AerialPercentage'
}, inplace = True)
# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')
# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)
# Name dataframe
dfPlayerMiscStats = df[2]

url = 'https://fbref.com/en/comps/9/playingtime/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)
df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s','MP','Starts','Compl','Min','PPM'], axis=1) # Drop unused columns

# Split duplicate columns due to "Per 90s" stats
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'Mn/MP': 'MinsPerMatch',
    'Min%': 'MinsPercentage',
    'Mn/Start': 'MinsPerStart',
    'Mn/Sub': 'MinsPerSub',
    'unSub': 'Benched',
    'onG': 'TeamGonPitch',
    'onGA': 'TeamGConPitch',
    '+/-': 'TeamG-TeamGConPitch',
    '+/-90': 'TeamG-TeamGConPitch90',
    'On-Off': 'TeamGonPitch-TeamGNotonPitch90',
    'onxG': 'xTeamGonPitch',
    'onxGA': 'xTeamGConPitch',
    'xG+/-': 'xTeamG-xTeamGConPitch',
    'xG+/-90': 'xTeamG-xTeamGConPitch90',
    'On-Off1': 'xTeamGonPitch-xTeamGNotonPitch90'
    }, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfPlayingTimeStats = df[2]

# Defensive Actions
url = 'https://fbref.com/en/comps/9/defense/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s'], axis=1) # Drop unused columns

# Split duplicate columns due to "Per 90s" stats
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)

# Rename columns that show per 90, and clearify other columns
df[2].rename(columns = {
    'Tkl': 'TackledPlayers',
    'TklW': 'TacklesWinBall',
    'Def 3rd': 'TacklesDefThird',
    'Mid 3rd': 'TacklesMidThird',
    'Att 3rd': 'TacklesAttThird',
    'Tkl1': 'DriblersTackled',
    'Att': 'Dribbles+TacklesLost',
    'Tkl%': 'DribblersTackledPercentage',
    'Past': 'DribbledPast',
    'Press': 'Presses',
    'Succ': 'PressesSuccessful',
    '%': 'PressesSuccessfulPercentage',
    'Def 3rd1': 'PressesDefThird',
    'Mid 3rd1': 'PressesMidThird',
    'Att 3rd1': 'PressesAttThird',
    'Blocks': 'BlockedBall',
    'Sh': 'BlockedShots',
    'ShSv': 'BlockedSoT',
    'Pass': 'BlockedPasses',
    'Int': 'Interceptions',
    'Tkl+Int': 'Tackles+Interceptions',
    'Clr': 'Clearances',
    'Err': 'ErrortoShot'
}, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfPlayerDefensiveStats = df[2]

url = 'https://fbref.com/en/comps/9/shooting/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s','PK','PKatt','npxG','xG','Gls'], axis=1) # Drop unused columns

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'Sh': 'ShotsTotal',
    'Sh/90': 'Shots90',
    'SoT/90': 'SoT90',
    'G/Sh': 'GoalsPerShot',
    'Dist': 'ShotDistanceAverage',
    'npxG/Sh': 'npxGPerShot',
    'FK': 'FKShot',
    'np:G-xG': 'npG-npxG'
}, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfPlayerShootingStats = df[2]

# Goal and Shot Creation
url = 'https://fbref.com/en/comps/9/gca/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s'], axis=1) # Drop unused columns

# Split duplicate columns due to "Per 90s" stats
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'SCA': 'ChancesCreated',
    'SCA90': 'ChancesCreated90',
    'PassLive': 'LivePasstoShot',
    'PassDead': 'DeadPasstoShot',
    'Drib': 'DribbletoShot',
    'Sh': 'ShottoShot',
    'Fld': 'FoulstoShot',
    'Def': 'DefensiveActiontoShot',
    'GCA': 'BigChancesCreated',
    'GCA90': 'BigChancesCreated90',
    'PassLive1': 'LivePasstoGoal',
    'PassDead1': 'DeadPasstoGoal',
    'Drib1': 'DribbletoGoal',
    'Sh1': 'ShottoGoalShot',
    'Fld1': 'FoultoGoal',
    'Def1': 'DefensiveActiontoGoal'
}, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfPlayerGoalCreationStats = df[2]

# Passing
url = 'https://fbref.com/en/comps/9/passing/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s','Ast','xA'], axis=1) # Drop unused columns

# Split duplicate columns due to "Per 90s" stats
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'Cmp': 'PassesCompletedTotal',
    'Att': 'PassesAttempted',
    'Cmp%': 'PassCompletedPercentage',
    'TotDist': 'PassDistanceTotal',
    'PrgDist': 'PassDistanceProgressive',
    'Cmp1': 'PassCompletedShort',
    'Att1': 'PassAttemptedShort',
    'Cmp%1': 'PassCompletedShortPercentage',
    'Cmp2': 'PassCompletedMedium',
    'Att2': 'PassAttemptedMedium',
    'Cmp%2': 'PassCompletedMediumPercentage',
    'Cmp3': 'PassCompletedLong',
    'Att3': 'PassAttemptedLong',
    'Cmp%3': 'PassCompletedLongPercentage',
    'KP': 'KeyPasses',
    '1/3': 'PassIntoAttThird',
    'PPA': 'PassCompleted18Yard',
    'CrsPA': 'CrossesCompleted18Yard',
    'Prog': 'PassProgressive'
}
, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfPlayerPassingStats = df[2]

# Pass Types
url = 'https://fbref.com/en/comps/9/passing_types/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s','Att','Cmp'], axis=1) # Drop unused columns

# Split duplicate columns due to "Per 90s" stats
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'Live': 'PassLiveBall',
    'Dead': 'PassDeadBall',
    'FK': 'PassFreeKick',
    'TB': 'PassDefensive',
    'Press': 'PassUnderPress',
    'Sw': 'PassWide',
    'Crs': 'Crosses',
    'CK': 'Corners',
    'In': 'CornersIn',
    'Out': 'CornersOut',
    'Str': 'CornersStraight',
    'Ground': 'PassGround',
    'Low': 'PassLow',
    'High': 'PassHigh',
    'Left': 'PassAttemptLeftFoot',
    'Right': 'PassAttemptRightFoot',
    'Head': 'PassAttemptHead',
    'TI': 'ThrowInsTaken',
    'Off': 'PasstoOffside',
    'Out': 'PasstoOutbound',
    'Int': 'PassIntercepted',
    'Blocks': 'PassBlocked'
}
, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfPlayerPassTypeStats = df[2]

# Possession
url = 'https://fbref.com/en/comps/9/possession/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s'], axis=1) # Drop unused columns

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'Def Pen': 'TouchesDefPen',
    'Def 3rd': 'TouchesDefThird',
    'Mid 3rd': 'TouchesMidThird',
    'Att 3rd': 'TouchesAttThird',
    'Att Pen': 'TouchesAttPen',
    'Succ': 'DribbleSuccess',
    'Att': 'DribbleAttempt',
    'Succ%': 'DribbleSuccessPercentage',
    '#PI': 'DribbledPlayers',
    'Megs': 'Nutmegs',
    'TotDist': 'CarriesTotalDistance',
    'PrgDist': 'CarriesProgressiveDistance',
    'Prog': 'CarriesProgressive',
    '1/3': 'CarriesAtt3rd',
    'CPA': 'Carries18Yard',
    'Mis': 'FailedBallAttempt',
    'Dis': 'LostBallTackle',
    'Targ': 'PassReceiveAttempt',
    'Rec': 'PassReceiveSuccess',
    'Rec%': 'PassReceiveSuccessPercentage',
    'Prog': 'PassReceiveProgressive'
}, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfPlayerPossessionStats = df[2]

# Goalkeeping Standard
url = 'https://fbref.com/en/comps/9/keepers/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s','MP','Starts','Min','W','D','L'], axis=1) # Drop unused columns

# Split duplicate columns
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'GA': 'GC',
    'GA90': 'GC90',
    'Save%': 'SoTSavePercentage',
    'Save%1': 'PenaltiesSavePercentage',
    'PKatt': 'PenaltiesAgainst',
    'PKA': 'PenaltiesAllowed',
    'PKsv': 'PenaltiesSaved',
    'PKm': 'PenaltiesMissed'
}
, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfKeeperStandard = df[2]

# Goalkeeping Advanced
url = 'https://fbref.com/en/comps/9/keepersadv/Premier-League-Stats'
html_content = requests.get(url).text.replace('<!--', '').replace('-->', '')
df = pd.read_html(html_content)

df[2].columns = df[2].columns.droplevel(0) # drop top header row
df[2] = df[2][df[2]['Rk'].ne('Rk')].reset_index() # remove mid-table header rows
df[2] = df[2].drop(['Matches','Rk','Nation','Pos','Squad','Age','Born','90s','GA','PKA'], axis=1) # Drop unused columns

# Split duplicate columns
s = pd.Series(df[2].columns)
df[2].columns= df[2].columns+s.groupby(s).cumcount().replace(0,'').astype(str)

# Rename columns to clearify column/stat
df[2].rename(columns = {
    'FK': 'GCFreeKick',
    'CK': 'GCCorner',
    'OG': 'GCOwnGoal',
    'PSxG': 'PostShotxG',
    'PSxG/SoT': 'PostShotxGPerSoT',
    'PSxG+/-': 'PostShotxG-GC',
    '/90': 'PostShotxG-GC90',
    'Cmp': 'PassCompleted40Y',
    'Att': 'PassAttempted40Y',
    'Cmp%': 'PassCompleted40YPercentage',
    'Att1': 'PassAttemptedGoalKick',
    'Thr': 'ThrowsAttempted',
    'Launch%': 'PassCompleted40Y%-GoalKick',
    'AvgLen': 'PassAvgLength',
    'Att2': 'GoalKickAttempted',
    'Launch%1': 'GoalKick40YPercentage',
    'AvgLen1': 'GoalKickAvgLength',
    'Opp': 'CrossesAttemptOpponentPenArea',
    'Stp': 'CrossesAttemptOpponentPenAreaStopped',
    'Stp%': 'CrossesAttemptOpponentPenAreaStoppedPercentage',
    '#OPA': 'DefensiveActionOutPenArea',
    '#OPA/90': 'DefensiveActionOutPenArea90',
    'AvgDist': 'AvgDistanceFromGoalDefensiveAction'
}
, inplace = True)

# Remove Duplicate Player Rows (Transfers intraleague during season)
df[2] = df[2].drop_duplicates(subset='Player')

# Reset Index
df[2] = df[2].reset_index()
df[2] = df[2].set_index('level_0')
df[2] = df[2].drop(['index'], axis=1)

# Name dataframe
dfKeeperAdvanced = df[2]

logger.info("Saving FBRef site data")
dfTeamStandardStats.to_sql(con=database_connection, name='teamstandard', if_exists='replace', index=False)
dfTeamVsStandardStats.to_sql(con=database_connection, name='opponentstandard', if_exists='replace', index=False)
dfPlayerStandardStats.to_sql(con=database_connection, name='playerstandard', if_exists='replace', index=False)
dfPlayerMiscStats.to_sql(con=database_connection, name='playersMisc', if_exists='replace', index=False)
dfPlayingTimeStats.to_sql(con=database_connection, name='playingtime', if_exists='replace', index=False)
dfPlayerDefensiveStats.to_sql(con=database_connection, name='defensivestats', if_exists='replace', index=False)
dfPlayerShootingStats.to_sql(con=database_connection, name='shootingstats', if_exists='replace', index=False)
dfPlayerGoalCreationStats.to_sql(con=database_connection, name='creativestats', if_exists='replace', index=False)
dfPlayerPassTypeStats.to_sql(con=database_connection, name='passtypestats', if_exists='replace', index=False)
dfPlayerPossessionStats.to_sql(con=database_connection, name='possessionstats', if_exists='replace', index=False)
dfKeeperStandard.to_sql(con=database_connection, name='keeperbasic', if_exists='replace', index=False)
dfKeeperAdvanced.to_sql(con=database_connection, name='keeperadvanced', if_exists='replace', index=False)

connection = database_connection.raw_connection()
cursor = connection.cursor()
cursor.callproc("fixPlayerNames")
cursor.close()
connection.commit()
