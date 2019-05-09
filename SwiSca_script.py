# -*- coding: utf-8 -*-
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
from http.client import IncompleteRead
from tweepy import API
import os
from my_twitter_tokens import consumer_key,consumer_secret,api_key,api_secret
from variables import words
import argparse

class listener(StreamListener):

    def __init__(self,api = None):
        self.api = api or API()
        todayDate = time.strftime('%Y%m%d')
        parser = argparse.ArgumentParser("archive")
        parser.add_argument("archive_dir", nargs='?', default='./', action="store",
                        help="a directory where results are stored")
        args = parser.parse_args()
        directory = './'+args.archive_dir+'/tweetdata/' + todayDate
        if not os.path.exists(directory):
           os.makedirs(directory)
        filenameStart = 'streamer.' + time.strftime('%Y%m%d-%H%M%S') + '.json'
        self.output  = open(os.path.join(directory,filenameStart).replace("\\","/"), 'ab')
        self.delout  = open('delete.txt', 'a')
        self.counter = 0
        
    def on_data(self, data):
        all_data = json.loads(data)
        if not "retweeted_status" in all_data:
            if 'geo' in all_data != False:
                if 'extended_tweet' in all_data:
                    if 'full_text' in all_data['extended_tweet']:
                        text = all_data['extended_tweet']['full_text']
                    else:
                        pass # i need to figure out what is possible here
                elif 'text' in all_data:
                    text = all_data['text']
                self.output.write(bytes(data,"UTF-8"))
                print(text+"\n")
                self.counter += 1
                
                if self.counter >= 500:
                        self.output.close()
                        directory = './tweetdata/' + time.strftime('%Y%m%d')
                        if not os.path.exists(directory): 
                            os.makedirs(directory)
                        self.output = open(os.path.join(directory,('streamer.' + time.strftime('%Y%m%d-%H%M%S') + '.json')).replace("\\","/"), 'ab')
                        self.counter = 0          
                return
            else:
                pass
        else:
            pass 
    def on_error(self, status):
        print(status)

auth = OAuthHandler(consumer_key.rstrip(), consumer_secret.rstrip())
auth.set_access_token(api_key.rstrip(), api_secret.rstrip())

while True:
    twitterStream = Stream(auth, listener(), tweet_mode='extended', timeout=60)
    #twitterStream.filter(locations=[19,59,32,70], stall_warnings = True)
    twitterStream.filter(track=words)
    #twitterStream.filter(languages=["sv","fi","no","da"], track = words)
    try:
        twitterStream.userstream()
                
    except Exception as e:
        print("Error. Restarting Stream.... Error: ")
        print(e.__doc__)
        print(e.message)

