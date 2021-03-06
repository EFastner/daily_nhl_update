require(RCurl); require(rjson); require(dplyr);
require(lubridate); require(doMC); require(rvest);
require(readr); library(RPostgreSQL); library(magrittr)

setwd('~/R/Scripts/daily_nhl_updates')

#Source all relevant scripts
source('psql_config.r', echo = FALSE) #PSQL credentials
source('daily_PbP_scrape.r', echo = FALSE) #Raw pbp scrape
source('daily_game_results.r', echo = FALSE) #Team and game summaries
source('daily_skater_summaries.r', echo = FALSE) #Skater Summaries

#Pull previous day's pbp files and load into sql database
#All functions located in daily_PbP_scrape.r
df.raw_pbp <- 
  fun.update_PbP_tables(start_date = as.Date(today()-1), end_date = as.Date(today()-1))

#Run if the previous function returned pbp data
if (!is.na(df.raw_pbp)){
  
  #Summarize results by team
  df.game_results <-
    fun.game_result_summary(df.raw_pbp[[1]])
  
  df.team_summary <- 
    fun.results_by_team(df.game_results)
  
  #Summarize results by skater
  df.skater_summary <-   
    fun.skater_summary(df.raw_pbp[[1]])
  
  df.combined_skaters <- 
    fun.combine_skater_stats(df.skater_summary)
  
  #Create connection to Postgres Database
  psql.connection <- 
    fun.postgres_connect(db.name = psql.db, 
                         db.host = psql.host, 
                         db.user = psql.user, 
                         db.password = psql.password)
  
  #make sure that all column names are lowercase so they can be loaded into postgres
  names(df.raw_pbp[[1]]) <- 
    tolower(names(df.raw_pbp[[1]]))
  
  names(df.raw_pbp[[2]]) <- 
    tolower(names(df.raw_pbp[[2]]))
  
  names(df.game_results) <- 
    tolower(names(df.game_results))
  
  names(df.combined_skaters) <- 
    tolower(names(df.combined_skaters))
  
  #Append raw_pbp table
  fun.append_table(connection = psql.connection, 
                   table = 'raw_pbp', 
                   values = df.raw_pbp[[1]])
  
  #Append roster table
  fun.append_table(connection = psql.connection, 
                   table = 'pbp_rosters', 
                   values = df.raw_pbp[[2]])
  
  #Append game results
  fun.append_table(connection = psql.connection, 
                   table = 'game_results', 
                   values = df.game_results)
  
  #Append team results
  fun.append_table(connection = psql.connection, 
                   table = 'team_results', 
                   values = df.team_summary)
  
  #Append skater stats
  fun.append_table(connection = psql.connection, 
                   table = 'skater_stats', 
                   values = df.combined_skaters)
}
