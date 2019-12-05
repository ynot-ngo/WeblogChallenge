#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import gzip

spark = SparkSession.builder.appName('LogAnalyzer').getOrCreate()

# Set column names
col_names = ["timestamp", "elb", "client:port", "backend:port", "request_processing_time", "backend_processing_time",
                "response_processing_time", "elb_status_code", "backend_status_code", "received_bytes", "sent_bytes", 
                "request", "user_agent", "ssl_cipher", "ssl_protocol"]

# Import data
df = pd.read_csv('data/2015_07_22_mktplace_shop_web_log_sample.log.gz', delim_whitespace=True, names=col_names, compression='gzip')

# Ensure record count is consistent to file
print(df.count())


# In[2]:


from pyspark.sql.functions import to_timestamp

# Create df of features needed for analysis
df_sessions = df.loc[:,[ "client:port", "request", "timestamp"]]

# Strip out port from client:port to create "unique user IP" 
df_sessions["client_ip"] = df_sessions["client:port"].str.split(":").str[0]
df_sessions.drop("client:port", axis=1, inplace=True)

# Format timestamp from ISO-8601 string to datetime format 
df_sessions["timestamp"] = pd.to_datetime(df_sessions["timestamp"])

# Sort data (required later)
df_sessions = df_sessions.sort_values(by="timestamp")


# In[3]:


from pyspark.sql.types import *

# Prepare schema for temp table
fields = [StructField("request", StringType(), True), StructField("timestamp", TimestampType(), True), StructField("client_ip", StringType(), True) ]
schema = StructType(fields)
# Apply schema
sparkDfSessions = spark.createDataFrame(df_sessions, schema)
sparkDfSessions.registerTempTable("weblogSessions")


# In[4]:


spark.sql('SELECT * FROM weblogSessions').show()


# In[5]:


# Determine the previous timestamp entry by using LAG window func as 
# data grouped by client_ip as timestamp in ascending order

weblogSessions_timestamps_df = spark.sql("""
    SELECT client_ip, request, timestamp, 
        LAG(timestamp) OVER (PARTITION BY client_ip ORDER BY timestamp) AS previous_timestamp
    FROM weblogSessions""")

weblogSessions_timestamps_df.registerTempTable("weblogSessions_timestamps")
spark.sql('select client_ip, timestamp, previous_timestamp from weblogSessions_timestamps').show(truncate=False)


# # Part 1. Sessionize the web log by IP.

# In[6]:


# Calculated using current - previous timestamp within unique client_ip 
# and continuous sesion distinguished via session threshold
# using 15 mins as session interval (can try standard 30 mins intervals also)

weblogSessions_sessions_df = spark.sql("""    
    SELECT *,
        CASE 
            WHEN unix_timestamp(timestamp) - unix_timestamp(previous_timestamp) >= (60 * 15) 
            OR previous_timestamp IS NULL
        THEN 1 ELSE 0 END AS is_new_session
    FROM weblogSessions_timestamps""")
weblogSessions_sessions_df.registerTempTable("weblogSessions_sessions")
# spark.sql("select client_ip, timestamp, previous_timestamp, is_new_session from weblogSessions_sessions").show(truncate=False)


# In[7]:


# Create id per sessions (running total of is_new_session)

weblogSessions_sessionIds_df = spark.sql("""    
    SELECT *, SUM(is_new_session) OVER (PARTITION BY client_ip ORDER BY timestamp) AS session_id
    FROM weblogSessions_sessions
    """)
weblogSessions_sessionIds_df.registerTempTable("weblogSessions_sessionIds")


# # Part 2. Determine the average session time.

# In[8]:


# Calculate the total session time across all sessions (based on client_ip and session_id)
# Sum the total number of sessions (with threshold 15 mins) per client_ip
# Divide total session time by total number of sessions for avg session time.

spark.sql("""
SELECT 
    SUM(total_session_time_per_ip) AS amount_of_time_in_seconds, 
    SUM(num_of_sessions_per_ip) AS num_of_sessions,
    (SUM(total_session_time_per_ip) / SUM(num_of_sessions_per_ip)) / 60 AS average_session_time_in_mins
FROM (
    SELECT client_ip, MAX(session_id) AS num_of_sessions_per_ip, SUM(session_time) AS total_session_time_per_ip
    FROM (
        SELECT client_ip, session_id, MAX(unix_timestamp(timestamp)) - MIN(unix_timestamp(timestamp)) AS session_time
        FROM weblogSessions_sessionIds
        GROUP BY client_ip, session_id 
        ORDER BY client_ip, session_id
    ) grouped_sessions
    GROUP BY client_ip
) average_sessions
""").show()


# # Part 3. Determine unique URL visits per session.

# In[9]:


# Determine distinct URL requests by clients per session.
# Number of unique requests per client divided by the total number of sessions per client is unique URL per session

spark.sql("""    
    SELECT
        client_ip, MAX(session_id) AS total_sessions_per_ip, COUNT(request) AS total_unique_visits,
        COUNT(request) / MAX(session_id) AS unique_visits_per_session
    FROM (
        SELECT DISTINCT client_ip, session_id, request
        FROM weblogSessions_sessionIds  
    ) sessions
    GROUP BY client_ip
    """).show()


# # Part 4. Find the most engaged users, ie the IPs with the longest session times

# In[10]:


# Group by client_ip and their sessions, and calculate the longest session they held

spark.sql("""
    SELECT client_ip, session_id, (MAX(unix_timestamp(timestamp)) - MIN(unix_timestamp(timestamp))) / 60 AS session_time_in_mins
    FROM weblogSessions_sessionIds
    GROUP BY client_ip, session_id
    ORDER BY session_time_in_mins DESC
""").show()


# # Additional Notes:
# 
# IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions.
# 
# There are additional methods for identifying visitors, sessions and paths:
# 
# 1. IP-Agent, in addition to IP address you can use user-agent (browser) information to determine unique sessions. This will help detect multiple users from one IP address. 
# 
# 2. Username data if the website is configured to require authentication.
# 
# 3. Session ID, if found in URL query or stored in a Cookie can result in more accurate visitor session and path analysis.
# 
# In addition, we can also filter entries which do not add value to the analytics. 
# Requests that are made by automatic activity such as bots can be filtered.
# Or some requests for components of the webpage such as to images may not be relevant.

# In[ ]:




