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
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Video_Id=item['id'],
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

def channel_tables(channel_names):

    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data",
                        port="5432")
    cursor=mydb.cursor()


    
    create_query="""create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(90) primary key,
                                                        Subscription_Count bigint,
                                                        Channel_Views bigint,
                                                        Total_videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))"""
    cursor.execute(create_query)
    mydb.commit()
    

    single_channel_detail=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for ch_data in collection1.find({"channel_information.Channel_Name":channel_names},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])

    df_single_channel_detail=pd.DataFrame(single_channel_detail)

    for index,row in df_single_channel_detail.iterrows():
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
        
        
        cursor.execute(insert_query,values)
        mydb.commit()
        
        print("channels values are already inserted")


def videos_table(channel_names):


        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345678",
                        database="Youtube_Data",
                        port="5432")
        cursor=mydb.cursor()       

        create_query='''create table if not exists videos(Channel_Name varchar(200),
                                                        Video_Id varchar(50) primary key,
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

        single_video_details=[]
        db=client["Youtube_Data"]
        collection1=db["Channel_Details"]
        for ch_data in collection1.find({"channel_information.Channel_Name":channel_names},{"_id":0}):
            single_video_details.append(ch_data["video_information"])

        df_single_video_details=pd.DataFrame(single_video_details[0])

        for index,row in df_single_video_details.iterrows():
                insert_query="""insert into videos(Channel_Name,
                                Video_Id,
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
                                
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


                values=(row['Channel_Name'],
                        row['Video_Id'],
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
                        row['Caption_Status'])


                cursor.execute(insert_query,values)
                mydb.commit()


def comments_table(channel_names):

    mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="12345678",
                    database="Youtube_Data",
                    port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published_At timestamp
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    single_comment_details=[]
    db=client["Youtube_Data"]
    collection1=db["Channel_Details"]
    for ch_data in collection1.find({"channel_information.Channel_Name":channel_names},{"_id":0}):
        single_comment_details.append(ch_data["comment_information"])

    df_single_comment_details=pd.DataFrame(single_comment_details[0])
    



    for index,row in df_single_comment_details.iterrows():
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


def tables(single_channel):
    channel_tables(single_channel)
    videos_table(single_channel)
    comments_table(single_channel)

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
    st.title(":black[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
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


all_channels=[]
db=client["Youtube_Data"]
collection1=db["Channel_Details"]
for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data["channel_information"]['Channel_Name'])

unique_channels=st.selectbox("Select the channel",all_channels)

if st.button("Migrate to SQL"):
    Table=tables(unique_channels)
    st.success(Table)

show_table=st.radio("Select The Table To be Viewed",("CHANNELS","VIDEOS","COMMENTS"))

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
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video,and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes,and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video,and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel,and what are their corresponding channel names?",
                                              "8.what are the names of all channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel,and what are their corresponding channel names?",
                                              "10.which videos have the highest number of comments,and what are their corresponding channel names?"))

if question=="1.What are the names of all the videos and their corresponding channels?":

    query1="""select video_name as videos,channel_name as channelname from videos"""
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["Video Title","Channel Name"])
    st.write(df)


elif question=="2.Which channels have the most number of videos, and how many videos do they have?":

    query2="""select channel_name as channelname,total_videos as no_of_videos from channels 
              order by total_videos desc """
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=["Channel Name","No of videos"])
    st.write(df1)


elif question=="3.What are the top 10 most viewed videos and their respective channels?":

    query3="""select view_count as views,channel_name as channelname,video_name as videotitle from videos 
          where view_count is not null order by views desc limit 10 """
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df2=pd.DataFrame(t3,columns=["views","Channel name","Videotitle"])
    st.write(df2)


elif question=="4.How many comments were made on each video,and what are their corresponding video names?":

      query4="""select comment_count as no_of_comments,video_name as videos from videos where comment_count is not null """
      cursor.execute(query4)
      mydb.commit()
      t4=cursor.fetchall()
      df3=pd.DataFrame(t4,columns=["no of comments","Videotitle"])
      st.write(df3)


elif question=="5.Which videos have the highest number of likes,and what are their corresponding channel names?":
    query5="""select video_name as videotitle,channel_name as channelname,like_count as likecount from videos
            where like_count is not null order by like_count desc """
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df4=pd.DataFrame(t5,columns=["Videotitle","channelname","likecount"])
    st.write(df4)    


elif question=="6.What is the total number of likes and dislikes for each video,and what are their corresponding video names?":
    query6="""select like_count as likecount,video_name as videotitle from videos"""
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df5=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df5)    

elif question=="7.What is the total number of views for each channel,and what are their corresponding channel names?":
    query6="""select channel_name as channelname,channel_views as views from channels"""
    cursor.execute(query6)
    mydb.commit()
    t7=cursor.fetchall()
    df6=pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df6)

elif question=="8.what are the names of all channels that have published videos in the year 2022?":

    query8="""select video_name as video_title,publishes_at as videorelease,channel_name as channelname from videos
            where extract(year from publishes_at)=2022"""
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df7=pd.DataFrame(t8,columns=["video_title","published_date","channel_name"])
    st.write(df7)

    
elif question=="9.What is the average duration of all videos in each channel,and what are their corresponding channel names?":
    query9="""select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name"""
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df8=pd.DataFrame(t9,columns=["channelname","averageduration"])
    
    T9=[]
    for index,row in df8.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10.which videos have the highest number of comments,and what are their corresponding channel names?":
    query10="""select video_name as videotitle,channel_name as channelname,comment_count as comments from videos
        where comment_count is not null order by comment_count desc """
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df9=pd.DataFrame(t10,columns=["video_title","channel_name","comments"])
    st.write(df9)
