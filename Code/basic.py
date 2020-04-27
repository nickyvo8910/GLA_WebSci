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


# Basic Stream


class MyStreamListener(tweepy.StreamListener):
    # Check for connection
    def on_connect(self):
        print("Basic Stream Connected")
    # When has data

    def on_data(self, data):
        try:
            # Loading as json and insert into database
            tweet = json.loads(data)
            db[_COLLECTIONS[0]].insert_one(tweet)
            db[_COLLECTIONS[1]].insert_one(tweet)
        except RateLimitError:
            print("RateLimitErrorBASIC")
        except TweepError:
            print("TweepErrorBASIC")
        except Exception:
            print(traceback.format_exc())

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False


def streamBasic():
    print('StartBasic')
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.sample(languages=['en'], async=True)

if __name__ == '__main__':
    # Start as a process
    p = multiprocessing.Process(
        target=streamBasic, name="streamBasic")
    p.start()

    p.join(_RUNNING_TIME)


    # If thread is active
    if p.is_alive():
        print("{} {} {}".format("streamBasic has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p.terminate()
        p.join()
