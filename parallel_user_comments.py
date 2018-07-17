from __future__ import print_function
from pandas import DataFrame
from pprint import pprint
import pandas as pd
import requests
import json
import sys
import time
import os
import errno
import datetime
from multiprocessing import Pool
import csv

CHUNK_ATTEMPTS = 6
CHUNK_ATTEMPT_SLEEP = 10

cid = "2yrsl4jc6lt9vnzj1j7oc8f7qdlrp8"
dirToUserVideos = sys.argv[1]
chosenDirectory = sys.argv[2]


if not os.path.exists(os.path.dirname(dirToUserVideos+'/user_videos/')):
    os.makedirs(os.path.dirname(dirToUserVideos+'/user_videos/'))

if not os.path.exists(chosenDirectory+"/user_videos_comments/"):
    os.makedirs(chosenDirectory+"/user_videos_comments/")

reader = csv.DictReader(open(dirToUserVideos+'/user_videos/users_videos_details.csv', 'r'))
dict_list = []
for line in reader:
    dict_list.append(line)

#details = pd.read_csv(dirToUserVideos+'/user_videos/users_videos_details.csv', index_col = 0)
#print(details)

#IDs=details['vid']


def save(row):
    vid=row['vid']
    name=row['name']
    game=row['game']
    dic2 = {}

    videoID = []
    commenter = []
    time_stamp = []
    comment_list = []
    print(str(datetime.datetime.now())+": downloading chat messages for vod " + str(vid))
    try:
        vod_info = requests.get("https://api.twitch.tv/kraken/videos/v/" + str(vid), headers={"Client-ID": cid}).json()
        response = None
        while response == None or '_next' in response:
            query = ('cursor=' + response['_next']) if response != None and '_next' in response else 'content_offset_seconds=0'
            for i in range(0, CHUNK_ATTEMPTS):
                error = None
                try:
                    #time.sleep(1) # time delay in seconds
                    response = requests.get("https://api.twitch.tv/v5/videos/" + str(vid) + "/comments?" + query,headers={"Client-ID": cid}).json()
                except requests.exceptions.ConnectionError as e:
                    error = str(e)
#                else:
#                    if "errors" in response or not "comments" in response:
#                        error = "error received in chat message response: " + str(response)
                if error == None:
                    # messages += response["comments"]
                    for item in response["comments"]:
                        if (item.get('message') != None and item.get('message') != ''):
                            #print(item.get('content_id')+item.get('message').get('body'))
                            videoID.append(item.get('content_id'))
                            commenter.append(item.get('commenter').get('display_name'))
                            time_stamp.append(item.get('created_at'))
                            comment_list.append(item.get('message').get('body'))
                    break #if it works we break
                else:
                    print("\nerror while downloading chunk: " + error)
                    if i < CHUNK_ATTEMPTS - 1:
                        print("retrying in " + str(CHUNK_ATTEMPT_SLEEP) + " seconds ", end="")
                    print("(attempt " + str(i + 1) + "/" + str(CHUNK_ATTEMPTS) + ")")
                    if i < CHUNK_ATTEMPTS - 1:
                        time.sleep(CHUNK_ATTEMPT_SLEEP)
            #if error != None:
        #        sys.exit("max retries exceeded.")

        #f.write(json.dumps(messages))
        #f.close()

        dic2['vid'] = videoID
        dic2['commenter'] = commenter
        dic2['time'] = time_stamp
        dic2['comment'] = comment_list
        df2 = DataFrame(data = dic2)
        df2.drop_duplicates(subset = None, inplace = True)

        #df2.to_csv(chosenDirectory+"/user_videos_comments/" + str(vid) +"_comments.csv", sep=',')
        df2.to_csv(chosenDirectory+"/user_videos_comments/" + str(vid) + "_" + str(name) + "_" + str(game)+ "_comments.csv", sep=',')
    except:
        print(str(datetime.datetime.now())+" -- "+vid+ ": Video broken or dropped adding to csv")



with Pool(5) as p:
    p.map(save, dict_list)
