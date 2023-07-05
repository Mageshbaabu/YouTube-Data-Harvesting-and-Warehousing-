import mysql as mysql
import pandas as pd
import streamlit as st
from googleapiclient.errors import HttpError
from streamlit_option_menu import option_menu
import urllib.parse
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import mysql.connector as sql
from googleapiclient.discovery import build
import plotly.express as px
import re

#Details to Connect to Youtube API
api_key = "AIzaSyCaxkUz6tk__yendlltWRc189wADNnePW0"
youtube = build('youtube', 'v3', developerKey=api_key)

#Connection to Mongodb Atlas
password = urllib.parse.quote_plus("Welcome@1104")
uri = "mongodb+srv://Mageshbaabu:{}@cluster0.85jpanb.mongodb.net/?retryWrites=true&w=majority".format(password)
client = MongoClient(uri, server_api=ServerApi('1')).Youtube.Channel
client1 = MongoClient(uri, server_api=ServerApi('1')).Youtube.ChannelVideos
client2 = MongoClient(uri, server_api=ServerApi('1')).Youtube.ChannelCmts
Arch_client = MongoClient(uri, server_api=ServerApi('1')).Youtube.ArchChannel
Arch_client1 = MongoClient(uri, server_api=ServerApi('1')).Youtube.ArchChannelVideos
Arch_client2 = MongoClient(uri, server_api=ServerApi('1')).Youtube.ArchChannelCmts

#Connection to MYSQL Database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube'
)
mycursor = mydb.cursor(buffered=True)

# SETTING PAGE CONFIGURATIONS
st.set_page_config(
                page_title= "Youtube Data Harvesting and Warehousing | By",
                layout= "wide",
                initial_sidebar_state= "expanded",
                menu_items={'About': """# This app is created by *MageshBaabu*"""}
                )

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home", "Extract & Transform", "View"],
                           icons=["house-door-fill", "tools", "card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#021945","text-color":"#021945"},
                                   "icon": {"font-size": "15px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#021945"}})

def get_channel_details(youtube, channel_ids):
    try:
        all_data = []
        try:
            request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(channel_ids))
            response = request.execute()

            if 'items' not in response:
                st.write(f"Invalid channel id: {channel_ids}")
                st.error("Enter the correct 11-digit **channel_id**")
                return None

            for i in range(len(response['items'])):
                data = (
                        dict(CHANNEL_ID=response['items'][i]['id'],
                        CHANNEL_NAME=response['items'][i]['snippet']['title'],
                        PLAYLILST_ID=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                        SUBSCRIBERS=response['items'][i]['statistics']['subscriberCount'],
                        TOTAL_VIDEOS=response['items'][i]['statistics']['videoCount'],
                        TOTAL_VIEWS=response['items'][i]['statistics']['viewCount'])
                        )
                all_data.append(data)

            return all_data
        except HttpError as e:
            st.error('Server error (or) Check your internet connection (or) Please Try again after a few minutes',
                     icon='🚨')
            st.write('An error occurred: %s' % e)
            return None

    except:
        st.write('Please try again after sometime.')

def get_channel_videos(ch_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=ch_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

def get_video_details(v_ids, Cmt_Count):
    video_stats = []

    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + 50])).execute()
        for video in response['items']:
            convertedduration=0
            dislikecount=0

            duration = video['contentDetails']['duration']
            dislikecount = video['statistics'].get('dislikeCount')

            convertedduration = YTDurationToSeconds(duration)
            if (dislikecount == None):
                dislikecount=0

            video_details = dict(
                                CAPTION_STATUS=video['contentDetails']['caption'],
                                CHANNEL_ID=video['snippet']['channelId'],
                                COMMENT_COUNT=video['statistics'].get('commentCount'),
                                DISLIKE_COUNT=dislikecount,
                                DURATION_COUNT=convertedduration,
                                FAVORITE_COUNT=video['statistics']['favoriteCount'],
                                LIKE_COUNT=video['statistics'].get('likeCount'),
                                PLAYLIST_ID=" ",
                                PUBLISHED_DATE=video['snippet']['publishedAt'],
                                THUMBNAIL=video['snippet']['thumbnails']['default']['url'],
                                VIDEO_DESCRIPTION=video['snippet']['description'],
                                VIDEO_ID=video['id'],
                                VIDEO_NAME=video['snippet']['title'],
                                VIEW_COUNT=video['statistics']['viewCount'],
                                CHANNEL_NAME=video['snippet']['channelTitle']
                                )
            Cmt_Count = video['statistics'].get('commentCount'),
            video_stats.append(video_details)
    return video_stats

def get_comments_details(video_id, ch_id):
    replies = []

    # retrieve youtube video results
    video_response = youtube.commentThreads().list(
        part='snippet,replies',
        videoId=video_id
    ).execute()

    # iterate video response
    while video_response:
        for item in video_response['items']:
            # Extracting comments
            data = dict(
                        COMMENT_AUTHOR=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        COMMENT_ID=item['id'],
                        COMMENT_TEXT=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        COMMENT_TIME=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                        VIDEO_ID=item['snippet']['videoId']
                        )
            replies.append(data)

        # Again repeat
        if 'nextPageToken' in video_response:
            video_response = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id
            ).execute()
        else:
            break
    return replies

def Check_Cmt_Available(video_id):
    IsCmtAvail = 'N'
    response = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id).execute()
    for video in response['items']:
        Cmt_Count = video['statistics'].get('commentCount')

    if (Cmt_Count != '0'):
        IsCmtAvail='Y'
    else:
        IsCmtAvail = 'N'

    return IsCmtAvail

def YTDurationToSeconds(duration):
    match = re.match('PT((\d+)H)?((\d+)M)?((\d+)S)?', duration).groups()
    hours = int(match[1]) if match[1] else 0
    minutes = int(match[3]) if match[3] else 0
    seconds = int(match[5]) if match[5] else 0
    return hours * 3600 + minutes * 60 + seconds

def OldDatatoArchival(ch_id):
    for Channel_id in ch_id:
        myList = list(client.find({"CHANNEL_ID": Channel_id}, {'_id': 0}))
        if len(myList) != 0:
            Arch_client.insert_many(myList)
            client.delete_many({"CHANNEL_ID": Channel_id})

            for vid in client1.find({"CHANNEL_ID": Channel_id}):
                for i in client2.find({'VIDEO_ID': vid['VIDEO_ID']}):
                    myList2 = list(client2.find({'VIDEO_ID': vid['VIDEO_ID']}, {'_id': 0}))
                    if len(myList2) != 0:
                        Arch_client2.insert_many(myList2)
                        client2.delete_many({'VIDEO_ID': vid['VIDEO_ID']})
                        Video_id = vid['VIDEO_ID']

                        # move the data to arch for Comment dtls
                        Inssql3 = "insert into arch_comment select * from comment where VIDEO_ID = %s"
                        mycursor.execute(Inssql3, (Video_id,))
                        sql3 = "Delete FROM comment where VIDEO_ID = %s"
                        mycursor.execute(sql3, (Video_id, ))
                        mydb.commit()

                myList3 = list(client1.find({"CHANNEL_ID": Channel_id}))

                if len(myList3) != 0:
                    Arch_client1.insert_many(myList3)
                    client1.delete_many({"CHANNEL_ID": Channel_id})

            #move the data to arch for video dtls
            Inssql2 = "insert into arch_video select * from video where CHANNEL_ID = %s"
            mycursor.execute(Inssql2, (Channel_id,))
            sql2 = "Delete FROM video where CHANNEL_ID = %s"
            mycursor.execute(sql2, (Channel_id, ))
            mydb.commit()

            # move the data to arch for channel dtls
            Inssql1 = "insert into arch_channel select * FROM channel where CHANNEL_ID = %s"
            mycursor.execute(Inssql1, (Channel_id, ))
            sql1 = "Delete FROM channel where CHANNEL_ID = %s"
            mycursor.execute(sql1, (Channel_id, ))
            mydb.commit()

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in client.find():
        ch_name.append(i['CHANNEL_NAME'])
    return ch_name

# HOME PAGE
if selected == "Home":
    # Title Image
    col1, col2 = st.columns(2, gap='medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")

# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    Channel_id = ''
    tab1, tab2 = st.tabs(["$\huge 📝 EXTRACT $", "$\huge🚀 TRANSFORM $"])
    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
        Channel_id=ch_id
        details = None

        if ch_id and st.button("Extract Data"):
            details = get_channel_details(youtube, ch_id)
            if details != None:
                st.write(f'#### Extracted data from :green["{details[0]["CHANNEL_NAME"]}"] channel')
                st.table(details)

        try:
            if st.button("Upload to MongoDB"):
                OldDatatoArchival(Channel_id)
                with st.spinner('Please Wait for it...'):
                    Cmt_Count = 0
                    details = get_channel_details(youtube, ch_id)
                    V_ids = get_channel_videos(ch_id)
                    vid_details = get_video_details(V_ids, Cmt_Count)

                    def comments():
                        comment_details = []
                        for i in V_ids:
                            IsCmtAvail=''
                            IsCmtAvail=Check_Cmt_Available(i)
                            if(IsCmtAvail == 'Y'):
                                com_d = get_comments_details(i, ch_id)
                                client2.insert_many(com_d)

                    comments()
                    client.insert_many(details)
                    client1.insert_many(vid_details)
        except:
            print('Invalid Channel ID !!!')


    # TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel",options= ch_names)

        def insert_into_channels():
            query = """INSERT INTO CHANNEL(CHANNEL_ID,CHANNEL_NAME,PLAYLIST_ID,SUBSCRIBERS,TOTAL_VIDEOS,TOTAL_VIEWS) VALUES(%s,%s,%s,%s,%s,%s)"""
            for i in client.find({"CHANNEL_NAME": user_inp}, {'_id': 0}):
                mycursor.execute(query, tuple(i.values()))
                mydb.commit()

        def insert_into_videos():
            query1 = """INSERT INTO video(CAPTION_STATUS,CHANNEL_ID,COMMENT_COUNT,DISLIKE_COUNT,DURATION_COUNT,FAVORITE_COUNT,LIKE_COUNT,PLAYLIST_ID,PUBLISHED_DATE,THUMBNAIL,VIDEO_DESCRIPTION,VIDEO_ID,VIDEO_NAME,VIEW_COUNT,CHANNEL_NAME) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            for i in client1.find({"CHANNEL_NAME": user_inp}, {'_id': 0}):
                mycursor.execute(query1, tuple(i.values()))
                mydb.commit()

        def insert_into_Comment():
            query2 = """INSERT INTO comment(COMMENT_AUTHOR,COMMENT_ID,COMMENT_TEXT,COMMENT_TIME,VIDEO_ID) VALUES(%s,%s,%s,%s,%s)"""
            for vid in client1.find({"CHANNEL_NAME": user_inp}, {'_id': 0}):
                for i in client2.find({'VIDEO_ID': vid['VIDEO_ID']}, {'_id': 0}):
                    mycursor.execute(query2, tuple(i.values()))
                    mydb.commit()

        if st.button("Submit"):
            try:
                insert_into_channels()
                insert_into_videos()
                insert_into_Comment()
                st.success("Transformation to MySQL Successful !!")
            except:
                st.error("Channel details already transformed !!")

# VIEW PAGE
if selected == "View":
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
                             ['1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT VIDEO_NAME AS VideoName, CH.CHANNEL_NAME AS ChannelName
                                FROM VIDEO VD, CHANNEL CH
                                WHERE VD.CHANNEL_ID = CH.CHANNEL_ID
                                ORDER BY CH.CHANNEL_NAME""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT CHANNEL_NAME AS ChannelName, TOTAL_VIDEOS AS TotalVideos
                                FROM CHANNEL
                                ORDER BY TOTAL_VIDEOS DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        # st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT CH.CHANNEL_NAME AS ChannelName, VD.VIDEO_NAME AS VideoName, VD.VIEW_COUNT AS Views 
                                FROM VIDEO VD, CHANNEL CH
                                WHERE VD.CHANNEL_ID = CH.CHANNEL_ID
                                ORDER BY views DESC
                                LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT COMMENT_COUNT, VIDEO_NAME FROM VIDEO ORDER BY COMMENT_COUNT DESC  LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT LIKE_COUNT, CHANNEL_NAME FROM VIDEO ORDER BY LIKE_COUNT DESC LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT VIDEO_NAME AS VideoName, LIKE_COUNT AS Like_Count, DISLIKE_COUNT AS DisLike_Count
                                FROM video
                                ORDER BY LIKE_COUNT DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT CHANNEL_NAME AS Channel_Name, TOTAL_VIEWS AS Views
                                FROM channel
                                ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT CHANNEL_NAME AS Channel_Name FROM video
                                WHERE published_date LIKE '2022%'
                                GROUP BY channel_name
                                ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                                AVG(DURATION_COUNT)/60 AS "Average_Video_Duration (mins)"
                                FROM video
                                GROUP BY channel_name
                                ORDER BY AVG(DURATION_COUNT)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Avg video duration for channels :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, COMMENT_COUNT AS Comment
                                FROM VIDEO
                                ORDER BY COMMENT_COUNT DESC
                                LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)