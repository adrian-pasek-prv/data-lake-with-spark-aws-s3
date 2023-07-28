# Project Goals
The goal of the project is to provide a music streaming startup, Sparkify, a data lake that will enable efficient data processing due to increasing size of their operation. Sparkify stores user activity in form of JSON log on their S3 bucket, which we need to ingest, process and write back to S3 bucket.

# ETL Pipeline Overview
* Ingest Sparkify's user logs stored in JSON files on S3 bucket
* Use Spark hosted on AWS EMR cluster to clean and transform the data according to star schema paradigm
* Write the transformed data back to S3 as parquet files with proper partitioning

# Schema
### Fact Table
* songplays - records in log data associated with song plays i.e. records with page NextSong
  - _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_
### Dimension Tables
* users - users in the app
  - _user_id, first_name, last_name, gender, level_
* songs - songs in music database
  - _song_id, title, artist_id, year, duration_
* artists - artists in music database
  - _artist_id, name, location, lattitude, longitude_
* time - timestamps of records in songplays broken down into specific units
  - _start_time, hour, day, week, month, year, weekday_
# How to Run
- Make sure you have proper `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` keys in your `~/.aws/credentials` file 
- `python etl.py`
