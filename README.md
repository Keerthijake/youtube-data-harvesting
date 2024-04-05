1. Tools Install
Virtual code.
Jupyter notebook.
Python 3.11.0 or higher.
MySQL.
Youtube API key.

3. Requirement Libraries to Install
pip install google-api-python-client, mysql-connector-python, pandas, streamlit.
( pip install google-api-python-client mysql-connector-python pandas streamlit )

4. Import Libraries
Youtube API libraries

import googleapiclient.discovery
from googleapiclient.discovery import build
SQL libraries

import mysql.connector
pandas, 
import pandas as pd
import streamlit as st
4. E T L Process
a) Extract data
Extract the particular youtube channel data by using the youtube channel id, with the help of the youtube API developer console.
b) Process and Transform the data
After the extraction process, takes the required details from the extraction data and transform it into JSON format.
c) Load data
After the transformation process, also It has the option to migrate the data to MySQL database 
5. E D A Process and Framework
a) Access MySQL DB
Create a connection to the MySQL server and access the specified MySQL DataBase by using  library and access tables.
b) Filter the data
Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.
c) Visualization
Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show the output in Dataframe Table and Bar chart.
User Guide
Step 1. Data collection zone
Search channel_id, copy and paste on the input box and click the Get data and stored button in the Data collection zone.
Step 2. Data Migrate zone
Select the channel name and click the Migrate to MySQL button to migrate the specific channel data to the MySQL database in the Data Migrate zone.
Step 3. Channel Data Analysis zone
Select a Question from the dropdown option you can get the results in Dataframe format or bar chat format.
