# scrapePremierLeagueData
Scraping Premier League data from FPL API, FBRef and UnderStat and saving it in a MySQL Database

I use this to scrape Premier League Data from FPL API FBRef and Understat. The data is stored in a MySQL database.
I run this every week after the fixtures and overwrite the data. 

There is an issue with Player names from different data sources, I have a strored procedure on my database to clean up player names so they match against different sources. 
