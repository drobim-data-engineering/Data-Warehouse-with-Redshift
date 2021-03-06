# Data Engineering NanoDegree

## Author
Deivid Robim [Linkedin](https://www.linkedin.com/in/deivid-robim-200b3330/)

### Project 3: Data Warehouse with Amazon Redshift

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Structure
```
Data-Warehouse-with-Redshift
│   README.md              # Project description
|   requirements.txt       # Python dependencies
│
└───src                    # Source code
|   |
│   │  create_resources.py # Resources creation script
|   |  sql_objects.py      # Definition of all sql objects
|   |  etl.py              # ETL script
|   |  validation.py       # Validates data load
|   |  delete_resources.py # Resources deletion script
|   |  dwh.cfg             # Configuration file
```

### Requirements for running locally
- Python3
- AWS Account
- [Configured AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)

### Datasets

You'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

<br />

**Song dataset:**

It's a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).
Each file is in JSON format and contains metadata about a song and the artist of that song
```
{
    "num_songs":1,
    "artist_id":"ARD7TVE1187B99BFB1",
    "artist_latitude":null,
    "artist_longitude":null,
    "artist_location":"California - LA",
    "artist_name":"Casual",
    "song_id":"SOMZWCG12A8C13C480",
    "title":"I Didn't Mean To",
    "duration":218.93179,
    "year":0
 }
```

**Log dataset:**

It consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above.
These simulate activity logs from a music streaming app based on specified configurations.
```
{
   "artist":null,
   "auth":"Logged In",
   "firstName":"Walter",
   "gender":"M",
   "itemInSession":0,
   "lastName":"Frye",
   "length":null,
   "level":"free",
   "location":"San Francisco-Oakland-Hayward, CA",
   "method":"GET",
   "page":"Home",
   "registration":1540919166796.0,
   "sessionId":38,
   "song":null,
   "status":200,
   "ts":1541105830796,
   "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
   "userId":"39"
}
```
### Database Schema

Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis.

This includes the following tables.

### Fact Table
```
• songplays - records in log data associated with song plays i.e. records with page NextSong
  table schema: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```
### Dimension Tables
```
• users - users in the app
  table schema: user_id, first_name, last_name, gender, level

• songs - songs in music database
  table schema: song_id, title, artist_id, year, duration

• artists - artists in music database
  table schema: artist_id, name, location, latitude, longitude

• time - timestamps of records in songplays broken down into specific units
  table schema: start_time, hour, day, week, month, year, weekday
```
### Instructions for running locally

#### Clone repository to local machine
```
git clone https://github.com/drobim-data-engineering/Data-Warehouse-with-Redshift.git
```

#### Change directory to local repository
```
cd Data-Warehouse-with-Redshift
```

#### Create Python Virtual Environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

#### Edit dwh.cfg file

This file holds the configuration variables used on the scripts to create and configure the AWS resources.

These are the variables the user needs to set up before running the `etl.py` script.

```
KEY = <ENTER AWS ACCESS KEY>   # paste your user Access Key
SECRET = <ENTER AWS SECRET KEY>  # paste your user Secret Key
VPC_ID = <ENTER VPC ID>  # paste the VPC_ID you want to create the resources (If blank the first VPC on user's AWS account is considered)
```
<b>REMEMBER:</b> Never save your <b>AWS ACCESS KEY & SECRET KEY</b> on scripts.

This is just an experiment to get familiarized with AWS SDK for Python.

#### Run script
```
cd src/
python -m etl.py # Entry point to kick-off a series of processes from creating resources to running validation queries.
```
The `etl.py` script was designed to delete ALL AWS resources provisioned after running the validation step.

The execution of this script incur <b>REAL MONEY</b> costs so be aware of that.
