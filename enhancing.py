import datetime
import json
import multiprocessing
import time
import traceback

import pymongo
import tweepy
from pymongo import MongoClient
from tweepy import RateLimitError, TweepError

_RUNNING_TIME = 61 * 60

# Setting up tokens
consumer_key = "VIGGAj16rqqaEOEEJjVWaROpi"
consumer_secret = "HAqUGIRXMvRDkDTdH2fJ1pxsjoRARbepz5rsSeHJWYl2eve1Bl"
access_token = "969038491706363905-VFcCIGYkGRAeG6CPh1kGCPbj8fiHXwo"
access_token_secret = "SkEve79Dkoha1r7wVn34VLyHLendlenfAmemKOpBjxdow"
# Setting up API
# Tweepy
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
# MongoDB
mongo = MongoClient('localhost', 27017)
db = mongo['tweets']
_COLLECTIONS = ['fat', 'basic', 'enhancedStream', 'rest', 'geo']
_TRACKING=['christmas','santa','reindeer']



class MyEnhancedStream(tweepy.StreamListener):
    # Check for connection
    def on_connect(self):
        print("Enhanced Stream Connected")
    # When has data
    def on_data(self, data):
        try:
            # Loading as json and insert into database
            tweet = json.loads(data)
            db[_COLLECTIONS[0]].insert_one(tweet)
            db[_COLLECTIONS[2]].insert_one(tweet)

        except RateLimitError:
            print("RateLimitErrorENHANCED")
        except TweepError:
            print("TweepErrorENHANCED")
        except Exception:
            print(traceback.format_exc())


def streamEnhanced():
    print('StartEnhanced')
    myStreamListener = MyEnhancedStream()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(languages=['en'], track=_TRACKING, async=True)

if __name__ == '__main__':
    # Start as a process for time control

    p2 = multiprocessing.Process(
        target=streamEnhanced, name="streamEnhanced")
    p2.start()

    #Wait for the amount of the running time
    p2.join(_RUNNING_TIME)

    # If thread is active then terminate it

    if p2.is_alive():
        print("{} {} {}".format("streamEnhanced has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p2.terminate()
        p2.join()
