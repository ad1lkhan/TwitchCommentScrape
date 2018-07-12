from __future__ import print_function
from pandas import DataFrame
from pprint import pprint

import pandas
import requests
import json
import sys
import datetime
import csv
import os

cid = "2yrsl4jc6lt9vnzj1j7oc8f7qdlrp8"

chosenDirectory = sys.argv[1]

# Access list of games (this is made manually)
with open('games.csv') as games_list:
    reader = csv.reader(games_list)
    games = [r[0] for r in reader]
    games.pop(0)

# Access list of users
with open('streamers.csv') as channel_list:
    reader = csv.reader(channel_list)
    streamers = [r[0] for r in reader]
    streamers.pop(0)

# Uncomment next 3 lines, and line after vod_info = r.... to save metadeta as json
# file_name = "user_details" + ".json"
# f = open(file_name, "w")
# users_info = []

if not os.path.exists(chosenDirectory+'/user_videos'):
    os.makedirs(chosenDirectory+'/user_videos')

df1 = DataFrame()
for user in streamers:

    vid = []
    name = []
    game = []
    created = []
    title = []
    duration = []
    views = []

    for x in range(0, 1000, 100):
        vod_info = requests.get("https://api.twitch.tv/kraken/channels/" + user + "/videos?limit=100&offset=" + str(x) + "&broadcast_type=archive,highlight", headers={"Client-ID": cid}).json()
        # users_info.append(vod_info)  # we store the vod metadata in the first element of the message array

        for info in vod_info.get('videos'):
            if info.get('game') in games and info.get('length') > 2700 and info.get('views') > 99:
                url = info.get('url')
                vid.append(url[url.rfind('/')+1:])
                name.append(info.get('channel').get('name'))
                game.append(info.get('game'))
                created.append(info.get('created_at'))
                title.append(info.get('title'))
                duration.append(info.get('length'))
                # time = info.get('length')
                # duration.append(str(datetime.timedelta(seconds=time)))
                views.append(info.get('views'))

        dic = {}
        dic['vid'] = vid
        dic['name'] = name
        dic['game'] = game
        dic['date created'] = created
        dic['title'] = title
        dic['duration of broadcast'] = duration
        dic['views'] = views

        df2 = DataFrame(data = dic)
        df1 = df1.append(df2)

    # These 2 lines MUST be uncommented too IF you are saving the metadata
    # f.write(json.dumps(users_info))
    # f.close()

df1 = df1.sort_values(['name', 'game'], ascending=[1, 0])
df1.drop_duplicates(subset = None, inplace = True)
df1.to_csv(chosenDirectory+"/user_videos/users_videos_details.csv", sep=',')

# print()
# print("saving to " + file_name)
print("done!")
