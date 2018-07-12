from pandas import DataFrame

import pandas as pd
import requests
import json
import os
import csv
import datetime
import sys

cid = "2yrsl4jc6lt9vnzj1j7oc8f7qdlrp8"
chosenDirectory = '/Users/adil/Documents/Project/Crawler/'

# Access list of games (this is made manually)
with open('games.csv') as games_list:
    reader = csv.reader(games_list)
    games = [r[0] for r in reader]
    games.pop(0)

# Open self-populated csv file with tournamnet channels
with open('esports_channels.csv') as channel_list:
    reader = csv.reader(channel_list)
    channels = [r[0] for r in reader]
    channels.pop(0)

# Uncomment next 3 lines, and line after vod_info = r.... to save metadeta as json
# channel_videos = []
# file_name = "channel_videos" + ".json"
# f = open(file_name, "w")

if not os.path.exists(chosenDirectory+'/esports_videos'):
    os.makedirs(chosenDirectory+'/esports_videos')

for channel in channels:

    vid = []
    name = []
    game = []
    created = []
    title = []
    duration = []
    views = []

    for x in range(0,5000, 100):
        vod_info = requests.get("https://api.twitch.tv/kraken/channels/" + channel + "/videos?limit=100&offset=" + str(x) + "&broadcast_type=archive,highlight", headers={"Client-ID": cid}).json()
        # channel_videos.append(vod_info)

        for info in vod_info.get('videos'):
            if info.get('game') in games and info.get('length') > 2700 and info.get('views') > 99:  # in seconds 2700s = 45min
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
    df2 = df2.append(df2)
    df2.drop_duplicates(subset = None, inplace = True)
    df2.to_csv(chosenDirectory+"/esports_videos/" + channel + "_videos_details.csv", sep=',')
