# Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Purpose

The set of scripts will build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

# Description of files in this repo

ClusterSetup.ipynb - Python notebook to help set up AWS resources to be used for the ETL pipeline
create_tables.py - Python script to be run to drop/create all necessary tables in Redshift 
dwh.cfg - Configuration file with all information used to connect to the Redshift cluster as well as the S3 locations of files
etl.py - ETL script to iterate load the data from S3 into Redshift staging tables and then insert that same data from staging tables into fact and dimension tables
README.md - This file containing information about this repository
sql_queries.py - Python script containing SQL commands used in create_tables.py and etl.py

# Database Design

There are five tables in the schema for this database

songs - Stores a unique list of songs with id, artist, title, year and duration information
artists - Stores a unique list of artists with id, name, location, latitude and longitude
users - Stores a unique list of users that have played songs with id, first_name, last_name, gender, and level
time - Stores a list of play times broken out into columns for time of day, hour, day, week, month, year, weekday
songplays - Stores a list of song plays with information about the user when they played the song including location, level, user_agent, and session

Most analytic queries would be initially started against the songplays table with joins being placed against the other dimension tables for information about the song, artist, user or time that it was played. This is why the songplays table is considered the fact table and the other tables are considered dimensions building out a star schema. 

# ETL pipeline

The ETL pipeline has two main methods
1) Load staging tables using S3 to copy the data from configured buckets into the two staging tables
2) Extract, transform, and load the data from the staging tables into the star schema tables listed above

The pipeline is designed as such to extract the data from the staging tables, transform it by removing duplicates and filtering out unnecessary events before inserting into the fact and dimension tables. 

# How to use

There is a ClusterSetup playbook that will help create the Redshift cluster programmatically. If you already have one then skip to step 3:

1) Edit dwh.cfg to set AWS key and secret for creation of your IAM role and Redshift cluster
2) Run through ClusterSetup.ipynb
3) Edit dwh.cfg to setup everything such as [CLUSTER][HOST] and [IAM_ROLE][ARN]. These can either be setup manually before hand or by using the ClusterSetup python notebook.
4) Run 'python create_tables.py' - This will set up the database tables and drop them first if they already exist
5) Run 'python etl.py' - This will load the data from S3 into staging tables and then extract, transform, and load the data into fact and dimension tables 

# Example queries

1) How many times was a song played in the last two hours of the day broken down by membership level?
```
select sp_level, count(*) from songplays join time on sp_start_time = t_start_time where t_hour = 23 or t_hour = 22 group by sp_level
```
sp_level,count
free,2
paid,16


2) Which users play multiple songs and how many do they play in the same session from where during the morning commute ?

```
select sp_user_id, sp_location, sp_session_id, count(*) from songplays 
join time on sp_start_time = t_start_time 
join users on sp_user_id = u_user_id 
where t_hour > 5 and t_hour < 7
group by sp_user_id, sp_location, sp_session_id
having count(*) > 1 
order by sp_location;
```
sp_user_id,sp_location,sp_session_id,count
25,"Marinette, WI-MI",128,2
49,"San Francisco-Oakland-Hayward, CA",1079,2

