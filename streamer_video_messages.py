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

CHUNK_ATTEMPTS = 6
CHUNK_ATTEMPT_SLEEP = 10

cid = "2yrsl4jc6lt9vnzj1j7oc8f7qdlrp8"
dirToUserVideos = sys.argv[1]
chosenDirectory = sys.argv[2]


if not os.path.exists(os.path.dirname(dirToUserVideos+'/user_videos/')):
    os.makedirs(os.path.dirname(dirToUserVideos+'/user_videos/'))

if not os.path.exists(chosenDirectory+"/user_videos_comments/"):
    os.makedirs(chosenDirectory+"/user_videos_comments/")


details = pd.read_csv(dirToUserVideos+'/user_videos/users_videos_details.csv', index_col = 0)
print(details)

grouped = details.groupby(['name','game'])



for name, group in grouped:
    ids  = group['vid'].tolist()

    dic2 = {}
    vid = []
    commenter = []
    time_stamp = []
    comment_list = []

    try:
        for num in ids:
            vod_info = requests.get("https://api.twitch.tv/kraken/videos/v/" + str(num), headers={"Client-ID": cid}).json()

            response = None
            print(str(datetime.datetime.now())+": downloading chat messages for vod " + str(num))# + game + ',' + num)
            while response == None or '_next' in response:
                query = ('cursor=' + response['_next']) if response != None and '_next' in response else 'content_offset_seconds=0'
                for i in range(0, CHUNK_ATTEMPTS):
                    error = None
                    try:
                        # start_time = time.time() # time this loop starts
                        time.sleep(1) # time delay in seconds
                        response = requests.get("https://api.twitch.tv/v5/videos/" + str(num) + "/comments?" + query,
                                                headers={"Client-ID": cid}).json()
                    except requests.exceptions.ConnectionError as e:
                        error = str(e)
                    else:
                        if "errors" in response or not "comments" in response:
                            error = "error received in chat message response: " + str(response)

                    if error == None:
                        # The original only had the next line! Additional lines added before next else statement
                        # messages += response["comments"]
                        for item in response["comments"]:
                            if (item.get('message') != None and item.get('message') != ''):
                                vid.append(item.get('content_id'))
                                commenter.append(item.get('commenter').get('display_name'))
                                time_stamp.append(item.get('created_at'))
                                comment_list.append(item.get('message').get('body'))
                        break
                    else:
                        print("\nerror while downloading chunk: " + error)

                        if i < CHUNK_ATTEMPTS - 1:
                            print("retrying in " + str(CHUNK_ATTEMPT_SLEEP) + " seconds ", end="")
                        print("(attempt " + str(i + 1) + "/" + str(CHUNK_ATTEMPTS) + ")")

                        if i < CHUNK_ATTEMPTS - 1:
                            time.sleep(CHUNK_ATTEMPT_SLEEP)
                # time taken for this loop to execute
                # print("time taken to execute loop took ", time.time() - start_time)
                if error != None:
                    sys.exit("max retries exceeded.")

        #f.write(json.dumps(messages))
        #f.close()
        # print()
        # print("saving to " + file_name)
        # print("done2!")

        dic2['vid'] = vid
        dic2['commenter'] = commenter
        dic2['time'] = time_stamp
        dic2['comment'] = comment_list

        df2 = DataFrame(data = dic2)
        df2 = df2.append(df2)
        df2.drop_duplicates(subset = None, inplace = True)
        df2.to_csv(chosenDirectory+"/user_videos_comments/" + name[0] + "_" + name[1] + "_" + str(time.time()) + "_comments.csv", sep=',')
    except:
        print(str(datetime.datetime.now())+": Video broken or something")
