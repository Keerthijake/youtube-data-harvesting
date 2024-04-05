import mysql.connector
import pandas as pd
import streamlit as st
import googleapiclient.discovery

#MYSQL CONNECTION
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Degree@1996",
    database="python3"
)


api_key = "AIzaSyClJcBnVZJX0s2wrVrMiJSo6Adt9LBapiM"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# channel details
def get_channel_stats(c1_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=c1_id
    )
    response = request.execute()
    channel_data = []
    for i in response["items"]:
        channel_data.append({
            "Channel_name": i["snippet"]["title"],
            "Channel_Id": i["id"],
            "Subscriber": i["statistics"]["subscriberCount"],
            "Views": i["statistics"]["viewCount"],
            "Video": i["statistics"]["videoCount"],
            "Channel_Description": i["snippet"]["description"],
            "Playlist_Id": i["contentDetails"]["relatedPlaylists"]["uploads"]
        })
    return channel_data

# video IDs of a channel
def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(
        id=channel_id, 
        part='contentDetails'
    ).execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        res = youtube.playlistItems().list(
            playlistId=playlist_id, 
            part='snippet', 
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in res['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# video details
def get_video_details(video_ids):
    video_stats = []
    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        ).execute()
        for video in response['items']:
            video_stats.append({
                "Video_id": video['id'],
                "Channel_name": video['snippet']['channelTitle'],
                "Channel_id": video['snippet']['channelId'],
                "Title": video['snippet']['title'],
                "Description": video['snippet']['description'],
                "Views_count": video['statistics']['viewCount'],
                "Likes": video['statistics'].get('likeCount'),
                "Dislikes": video['statistics'].get('dislike'),
                "Comments": video['statistics'].get('commentCount'),
                "Fav_count": video['statistics']['favoriteCount'],
                "Published_date": video['snippet']['publishedAt'],
                "Duration": video['contentDetails']['duration']
            })
    return video_stats

#  comments of videos
def get_comments(video_ids):
    comments_data = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                textFormat="plainText",
                maxResults=100
            )
            response = request.execute()
            for item in response["items"]:
                comments_data.append({
                    "comment_id": item["id"],
                    "video_id": item["snippet"]["videoId"],
                    "comment_text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "comment_author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                })
        except:
            pass
    return comments_data


#create table

import mysql.connector

db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Degree@1996",
  database="python3"
)
def create_tables(conn):

    cursor = db.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS Channel(
                    Channel_name VARCHAR(255),
                    Channel_Id VARCHAR(255),
                    Subscriber INT,
                    Views INT,
                    Video INT,
                    Channel_Description TEXT,
                    Playlist_Id VARCHAR(255)
                    )''')

    cursor.execute('''CREATE TABLE  IF NOT EXISTS Video (
                    Video_id VARCHAR(255) PRIMARY KEY,
                    Channel_name VARCHAR(255),
                    Channel_id VARCHAR(255),
                    Title VARCHAR(255),
                    Description TEXT,
                    Views_count INT,
                    Likes INT,
                    Dislikes INT,
                    Comments INT,
                    Fav_count INT,
                    Published_date DATE,
                    Duration TIME
                    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Comment (
                    comment_id VARCHAR(255) PRIMARY KEY,
                    video_id VARCHAR(255),
                    comment_text TEXT,
                    comment_author VARCHAR(255)
                    )''')



    conn.commit()
    cursor.close()

# insert data into MySQL
def insert_data_into_mysql(conn, channel_data, video_data, comment_data):
    cursor = conn.cursor()

    for channel in channel_data:
        sql = "INSERT IGNORE INTO Channel (Channel_name, Channel_Id, Subscriber, Views, Video, Channel_Description, Playlist_Id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (
            channel['Channel_name'],
            channel['Channel_Id'],
            channel['Subscriber'],
            channel['Views'],
            channel['Video'],
            channel['Channel_Description'],
            channel['Playlist_Id']
        )
        cursor.execute(sql, val)


    for video in video_data:
        sql = "INSERT IGNORE INTO Video (Video_id, Channel_name, Channel_id, Title, Description, Views_count, Likes, Dislikes, Comments, Fav_count, Published_date, Duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
            video['Video_id'],
            video['Channel_name'],
            video['Channel_id'],
            video['Title'],
            video['Description'],
            video['Views_count'],
            video['Likes'],
            video['Dislikes'],
            video['Comments'],
            video['Fav_count'],
            video['Published_date'],
            video['Duration']
        )
        cursor.execute(sql, val)

    
    for comment in comment_data:
        sql = "INSERT IGNORE INTO Comment (comment_id, video_id, comment_text, comment_author) VALUES (%s, %s, %s, %s)"
        val = (
            comment['comment_id'],
            comment['video_id'],
            comment['comment_text'],
            comment['comment_author']
        )
        cursor.execute(sql, val)

    conn.commit()
    cursor.close()

def execute_selected_query(option):
    cursor = conn.cursor()

    if option == "What are the names of all the videos and their corresponding channels?":
        query = """
                SELECT Video.Title, Channel.Channel_name 
                FROM Channel 
                JOIN Video ON Channel.Channel_Id = Video.Channel_id
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Video Title', 'Channel Name'])
        st.write(df)

    elif option == "Which channels have the most number of videos, and how many videos do they have?":
        query = """
                SELECT Channel.Channel_name, COUNT(Video.Video_id) AS Number_of_videos 
                FROM Channel 
                JOIN Video ON Channel.Channel_Id = Video.Channel_id 
                GROUP BY Channel.Channel_name 
                ORDER BY Number_of_videos DESC
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Channel Name', 'Number of Videos'])
        st.write(df)

    elif option == "What are the top 10 most viewed videos and their respective channels?":
        query = """
                SELECT Video.Title, Channel.Channel_name 
                FROM Video 
                JOIN Channel ON Video.Channel_id = Channel.Channel_Id 
                ORDER BY Video.Views_count DESC 
                LIMIT 10
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Video Title', 'Channel Name'])
        st.write(df)

    elif option == "How many comments were made on each video, and what are their corresponding video names?":
        query = """
                SELECT Video.Title, COUNT(Comment.comment_id) AS Number_of_comments 
                FROM Video 
                LEFT JOIN Comment ON Video.Video_id = Comment.video_id 
                GROUP BY Video.Video_id, Video.Title
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Video Title', 'Number of Comments'])
        st.write(df)

    elif option == "Which videos have the highest number of likes, and what are their corresponding channel names?":
        query = """
                SELECT Video.Title, Channel.Channel_name, Video.Likes 
                FROM Video 
                JOIN Channel ON Video.Channel_id = Channel.Channel_Id 
                ORDER BY Video.Likes DESC
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Video Title', 'Channel Name', 'Likes'])
        st.write(df)

    elif option == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query = """
                SELECT Video.Title, SUM(Video.Likes) AS TotalLikes, SUM(Video.Dislikes) AS TotalDislikes
                FROM Video 
                GROUP BY Video.Title
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Title', 'Total_likes', 'Total_dislikes'])
        st.write(df)

    elif option == "What is the total number of views for each channel, and what are their corresponding channel names?":
        query = """
                SELECT Channel.Channel_name, SUM(Video.Views_count) AS TotalViews
                FROM Channel 
                JOIN Video ON Channel.Channel_Id = Video.Channel_id
                GROUP BY Channel.Channel_name
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Channel Name', 'Total Views'])
        st.write(df)

    elif option == "What are the names of all the channels that have published videos in the year 2022?":
        query = """
                SELECT DISTINCT Channel.Channel_name
                FROM Channel 
                JOIN Video ON Channel.Channel_Id = Video.Channel_id
                WHERE YEAR(Video.Published_date) = 2022
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['published_date'])
        st.write(df)

    elif option == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query = """
                SELECT Channel.Channel_name, AVG(Video.Duration) AS Average_Duration
                FROM Channel 
                JOIN Video ON Channel.Channel_Id = Video.Channel_id
                GROUP BY Channel.Channel_name
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Channel Name', 'Average Duration'])
        st.write(df)

    elif option == "Which videos have the highest number of comments, and what are their corresponding channel names?":
        query = """
                SELECT Channel.Channel_name, Video.Title, Video.Comments
                FROM Channel 
                JOIN Video ON Channel.Channel_Id = Video.Channel_id
                ORDER BY Video.Comments DESC
                LIMIT 10
            """
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=['Channel Name', 'Video Title', 'Number of Comments'])
        st.write(df)

    cursor.close()

def main():
    st.title("YouTube Data Analysis")
    channel_id = st.text_input("Enter the Channel ID:")
    
    if st.button("Scrape and Insert Data"):
        channel_data = get_channel_stats(channel_id)
        video_ids = get_channel_videos(channel_id)
        video_data = get_video_details(video_ids)
        comment_data = get_comments(video_ids)
        insert_data_into_mysql(conn, channel_data, video_data, comment_data)
        st.success("Data scraped and inserted into MySQL successfully")

    option = st.selectbox(
        "Select a query to execute:",
        [
            "What are the names of all the videos and their corresponding channels?",
            "Which channels have the most number of videos, and how many videos do they have?",
            "What are the top 10 most viewed videos and their respective channels?",
            "How many comments were made on each video, and what are their corresponding video names?",
            "Which videos have the highest number of likes, and what are their corresponding channel names?",
            "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "What is the total number of views for each channel, and what are their corresponding channel names?",
            "What are the names of all the channels that have published videos in the year 2022?",
            "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "Which videos have the highest number of comments, and what are their corresponding channel names?"
        ]
    )
    if st.button("Execute Query"):
        execute_selected_query(option)

if __name__ == "__main__":
    main()
