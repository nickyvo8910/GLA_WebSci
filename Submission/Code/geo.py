import datetime
import json
import multiprocessing
import time
import traceback

import pymongo
import tweepy
from pymongo import MongoClient
from tweepy import RateLimitError, TweepError

_RUNNING_TIME = 6 * 60


# Setting up API
# Tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
# MongoDB
mongo = MongoClient('localhost', 27017)
db = mongo['tweets']
_COLLECTIONS = ['fat', 'basic', 'enhancedStream', 'rest', 'geo']

class MyGeoStream(tweepy.StreamListener):
    # Check for connection
    def on_connect(self):
        print("Geo Stream Connected")
    # When has data

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            db[_COLLECTIONS[0]].insert_one(tweet)
            db[_COLLECTIONS[4]].insert_one(tweet)
        except RateLimitError:
            print("RateLimitErrorGEO")
        except TweepError:
            print("TweepErrorGEO")
        except Exception:
            print(traceback.format_exc())

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

def streamGeo():
    print("Starting Geo")
    myStreamListener = MyGeoStream()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(languages=['en'], locations=[-4.393201,
                                                 55.781277, -4.071717, 55.929638], async=True)



if __name__ == '__main__':
    # Start as a process
    p4 = multiprocessing.Process(
        target=streamGeo, name="streamGeo")
    p4.start()

    p4.join(_RUNNING_TIME)

    # If thread is active
    if p4.is_alive():
        print("{} {} {}".format("streamGeo has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p4.terminate()
        p4.join()
