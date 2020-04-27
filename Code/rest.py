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
_TRACKING=['christmas','santa','reindeer']

REST_call_left = 30


def restProbe(callLeft):
    print("Starting REST")
    try:
        while(callLeft > 0):
            myrestProbe = api.search(
                q=_TRACKING, languages=['en'], count=100)
            for tweet in myrestProbe:
                data = json.loads(json.dumps(tweet._json))
                db[_COLLECTIONS[0]].insert_one(data)
                db[_COLLECTIONS[3]].insert_one(data)
            callLeft -= 1
        if(callLeft == 0):
            print("DODGED")
            time.sleep(61*30)
            restProbe(30)
    except RateLimitError:
        print("RateLimitErrorREST")
        restProbe(0)
    except TweepError:
        print("TweepErrorREST")
    except Exception:
        print(traceback.format_exc())


if __name__ == '__main__':
    # Start as a process
    p3 = multiprocessing.Process(
        target=restProbe, name="restProbe", args=(REST_call_left,))
    p3.start()

    p3.join(_RUNNING_TIME)

    # If thread is active

    if p3.is_alive():
        print("{} {} {}".format("restProbe has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p3.terminate()
        p3.join()
