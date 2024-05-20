import googleapiclient.discovery
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
 

#api connection
api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyBjHfR4XiPDeQ8jKNwCYapRbVIHlLuAJMc"
youtube = googleapiclient.discovery.build(
api_service_name, api_version,developerKey=api_key)


#getting channel id info
def get_channel_info(channel_id):
 request=youtube.channels().list(
                    part='snippet,ContentDetails,statistics',
                    id=channel_id
    )
 response=request.execute()
 for i in response["items"]:
        data=dict(Channel_Name=i["snippet"]["title"],
                  Channel_Id=i["id"],
                  Subscription_Count=i["statistics"]["subscriberCount"],
                  Channel_Views=i["statistics"]["viewCount"],
                  Total_videos=i["statistics"]["videoCount"],
                  Channel_Description=i["snippet"]["description"],
                  Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
                  )
 return data


#getting video id
def get_video_ids(channel_id):
    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
                                
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None

    while True:                    
        response1=youtube.playlistItems().list(
                                part='snippet',
                                playlistId=playlist_Id,
                                maxResults=50,
                                pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
     
        if next_page_token is None:
          break
    return video_ids


#to get video info
def Get_Video_Info(video_ids):
    video_data=[]

    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Video_Id=item['id'],
                    Video_Name=item['snippet']['title'],
                    Video_Description=item['snippet'].get('description'),
                    Tags=item['snippet'].get('tags'),
                    Publishes_at=item['snippet']['publishedAt'],
                    View_Count=item['statistics'].get('viewCount'),
                    Like_Count=item['statistics'].get('likeCount'),
                    Favorite_Count=item['statistics']["favoriteCount"],
                    Comment_Count=item['statistics'].get("commentCount"),
                    Duration=item['contentDetails']['duration'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Caption_Status=item['contentDetails']['caption'],
                     )
            video_data.append(data)
    return  video_data


#get comment info
def get_comment_info(video_ids):
    Comment_Data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=50
                )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item["snippet"]["topLevelComment"]['id'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet'] ['authorDisplayName'],
                        Comment_Published_At=item['snippet']['topLevelComment']['snippet']['publishedAt'] )
                
                Comment_Data.append(data)
    except:
     pass
    return Comment_Data


#upload to mongodb

client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client['Youtube_Data']


def channel_details(channel_id):
    ch_details= get_channel_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=Get_Video_Info(vi_ids)
    com_details=get_comment_info(vi_ids)

    collection1=db["Channel_Details"]
    collection1.insert_one({"channel_information": ch_details,
                            "video_information":vi_details,
                            "comment_information":com_details
                            })
    return "upload successful"



#table creation for channels,videos and comments

def channel_tables():

    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data",
                        port="5432")
    cursor=mydb.cursor()

    drop_query="""drop table if exists channels"""
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_query="""create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(90) primary key,
                                                            Subscription_Count bigint,
                                                            Channel_Views bigint,
                                                            Total_videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channel table already created")

    ch_list=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
     ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query="""insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Channel_Views,
                                            Total_videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channels values are already inserted")


def videos_table():


        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data",
                        port="5432")
        cursor=mydb.cursor()

        drop_query="""drop table if exists videos"""
        cursor.execute(drop_query)
        mydb.commit()


        create_query='''create table if not exists videos(Video_Id varchar(50) primary key,
                                                        Video_Name varchar(150),
                                                        Video_Description text,
                                                        Tags text,
                                                        Publishes_at timestamp,
                                                        View_Count bigint,
                                                        Like_Count bigint,
                                                        Favorite_Count int,
                                                        Comment_Count int,
                                                        Duration interval,
                                                        Thumbnail varchar(200),
                                                        Caption_Status varchar(50)
                                                        )'''
        cursor.execute(create_query)
        mydb.commit()

        vi_list=[]
        db=client["Youtube_Data"]
        collection1=db["Channel_Details"]
        for vi_data in collection1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data['video_information'])):
                        vi_list.append(vi_data["video_information"][i])
                        df1=pd.DataFrame(vi_list)

        for index,row in df1.iterrows():
                insert_query="""insert into videos(Video_Id,
                                Video_Name,
                                Video_Description,
                                Tags,
                                Publishes_at,
                                View_Count,
                                Like_Count,
                                Favorite_Count,
                                Comment_Count,
                                Duration,
                                Thumbnail,
                                Caption_Status
                                )
                                
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


                values=(row['Video_Id'],
        row['Video_Name'],
        row['Video_Description'],
        row['Tags'],
        row['Publishes_at'],
        row['View_Count'],
        row['Like_Count'],
        row['Favorite_Count'],
        row['Comment_Count'],
        row['Duration'],
        row['Thumbnail'],
        row['Caption_Status']
        )


                cursor.execute(insert_query,values)
                mydb.commit()


def comments_table():

    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="12345678",
                    database="Youtube_Data",
                    port="5432")
    cursor=mydb.cursor()

    drop_query="""drop table if exists comments"""
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published_At timestamp
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

        
    com_list=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
            for i in range(len(com_data['comment_information'])):
                com_list.append(com_data["comment_information"][i])
                df2=pd.DataFrame(com_list)


    for index,row in df2.iterrows():
                    insert_query="""insert into comments(Comment_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published_At
                                                    )
                                                    
                                                    values(%s,%s,%s,%s)"""
            
            
                    values=(row['Comment_Id'],
                            row['Comment_Text'],
                            row['Comment_Author'],
                            row['Comment_Published_At']
                            )
                    
            
                    cursor.execute(insert_query,values)
                    mydb.commit()


def tables():
    channel_tables()
    videos_table()
    comments_table()

    return "Tables Created Successfully" 



#channel dataframe 
def show_channels_table():
    ch_list=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df
    

#video dataframe
def show_videos_table():
    vi_list=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data["video_information"][i])
    df1=st.dataframe(vi_list)
    return df1

#comment dataframe

def show_comments_table():
    com_list=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data["comment_information"][i])
    df2=st.dataframe(com_list)
    return df2


#streamlit part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python scripting")
    st.caption("Data collection")
    st.caption("API Integration")
    st.caption("Data Management using Mongodb and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("Collect and store the data"):
    ch_ids=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    
    if channel_id in ch_ids:
        st.success("The given Channel Id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("Select The Table To be Viewed",("CHANNELS","VIDEOS","CoOMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()


#sql connection
mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="12345678",
                database="Youtube_Data",
                port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video,and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes,and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video,and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel,and what are their corresponding channel names?",
                                              "8.what are the names of all channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel,and what are their corresponding channel names?",
                                              "10.which videos have the highest number of comments,and what are their corresponding channel names?"))