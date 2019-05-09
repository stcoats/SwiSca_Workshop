# -*- coding: utf-8 -*-

import folium
import pandas as pd
import html
import branca
import glob
from pandas.io.json import json_normalize
import re
import os
import webbrowser
import random
from variables import words

#df = pd.DataFrame(place_coords)
#df = pd.read_json("/home/cloud-user/taito_wrk/DONOTREMOVE/tweetdata/20170110/streamer.20170110-000126.json")
filepath = './map.html'
files = glob.glob("./tweetdata/*/streamer*.json")
def centr(coords):
    return [(coords[0][0][0]+coords[0][1][0])/2,(coords[0][0][1]+coords[0][1][1])/2]

url_regex = re.compile(r"""
       [^\s]             # not whitespace
       [a-zA-Z0-9:/\-]+  # the protocol and domain name
       \.(?!\.)          # A literal '.' not followed by another
       [\w\-\./\?=&%~#]+ # country and path components
       [^\s]             # not whitespace""", re.VERBOSE)

get_colors = lambda n: list(map(lambda i: "#" + "%06x" % random.randint(0, 0xFFFFFF),range(n)))

def linkify(raw_message):
    message = raw_message
    for url in url_regex.findall(raw_message):
        if url.endswith('.'):
            url = url[:-1]
        if 'http://' not in url:
            href = 'http://' + url
        if 'https://' not in url:
            href = 'https://' + url
        else:
            href = url
        message = message.replace(url, '<a href="%s" target="_blank">%s</a>' % (href, url))

    return message

locations_df = pd.DataFrame()
for x in files:
    df = pd.read_json(x, lines = True)
    try:
        df1 = df.loc[(df["coordinates"].notnull())|(df["place"].notnull())].reset_index(drop=True).copy()
        df1["coords"] = df1["coordinates"].fillna(df1["place"])
        df1["coords"] = json_normalize(df1["coords"])["coordinates"].fillna(json_normalize(df1["coords"])["bounding_box.coordinates"])#.apply(lambda x:centr(x))
        df1["coords"] = [x if len(x)!=1 else centr(x) for x in df1["coords"]]
        #df1["coords"] = b["coordinates"].copy()#json_normalize(df1["coordinates"])["coordinates"]
        #print(len(b),len(df1))
        #df.columns = ["text","lat","lon"]
        df1["tex"] = [x["text"] if str(x["extended_tweet"]) == "nan" else json_normalize(x["extended_tweet"])["full_text"][0] for i,x in df1.iterrows()]
        df1["coords"] = [x[::-1] for x in df1["coords"]]
        locations_df = locations_df.append(df1[["coords","tex"]])
    except:
        continue
locations_df["tex"] = locations_df["tex"].str.replace("´|`", "'")
locations_df = locations_df[locations_df.notnull()]
#create a map
this_map = folium.Map(prefer_canvas=True)

#words = ["shit","fuck","damn"]
colors = ['#e6194b', '#3cb44b', '#ffe119',
        '#4363d8', '#f58231', '#911eb4', '#46f0f0',
    '#f032e6', '#bcf60c', '#fabebe', '#008080',
    '#e6beff', '#9a6324', '#fffac8', '#800000',
    '#aaffc3', '#808000', '#ffd8b1', '#000075', '#808080']
#colors = get_colors(len(words))

for z,x in enumerate(words):
    df = locations_df[locations_df["tex"].str.contains("\\b"+x+".+?\\b", regex = True)]
    fgv = folium.FeatureGroup(name=x)
    for i,y in df.iterrows():
        
        test = folium.Html(linkify(y.tex), script=True) # i'm assuming this bit runs fine
        iframe = branca.element.IFrame(html=test, width=450, height=150)
        fgv.add_child(folium.CircleMarker(location=y.coords,
                            radius=4,
                            popup = folium.Popup(test, max_width=450, parse_html=True),
                            color = colors[z],
                            opacity = 0.5,
                            fill=True, # Set fill to True
                            fill_color=colors[z],
                            fill_opacity=0.5,
                            name=x))#.add_to(this_map)
        this_map.add_child(fgv)
folium.LayerControl().add_to(this_map)

this_map.fit_bounds(this_map.get_bounds())
#Set the zoom to the maximum possible
this_map.save(filepath)

#Save the map to an HTML file
this_map.save(filepath)
webbrowser.open('file://' + os.path.abspath(filepath))

