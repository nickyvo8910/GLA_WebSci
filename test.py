import datetime
import pymongo
from pymongo import MongoClient
import tweepy
from tweepy import TweepError, RateLimitError
import json
import traceback

import multiprocessing
import time

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
db = mongo['tweet']
_COLLECTIONS = ['fat', 'basic', 'enhancedStream', 'rest', 'geo']

# Basic Stream


class MyStreamListener(tweepy.StreamListener):
    # Check for connection
    def on_connect(self):
        print("Connected")
    # When has data

    def on_data(self, data):
        try:
            # Loading as json and insert into database
            tweet = json.loads(data)
            db[_COLLECTIONS[0]].insert_one(tweet)
            db[_COLLECTIONS[1]].insert_one(tweet)
            ## DEBUG: OK
            print("basic_insert_one")
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())


class MyEnhancedStream(tweepy.StreamListener):
    # Check for connection
    def on_connect(self):
        print("Connected")
    # When has data

    def on_data(self, data):
        try:
            # Loading as json and insert into database
            tweet = json.loads(data)
            db[_COLLECTIONS[0]].insert_one(tweet)
            db[_COLLECTIONS[2]].insert_one(tweet)
            ## DEBUG: OK
            print("streamDb_insert_one")
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())


class MyGeoStream(tweepy.StreamListener):
    def on_data(self, data):
        try:
            tweet = json.loads(data)
            db[_COLLECTIONS[0]].insert_one(tweet)
            db[_COLLECTIONS[4]].insert_one(tweet)
            print("geoDb---inserted")
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())


def streamBasic():
    print('StartBasic')
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.sample(languages=['en'], async=True)


def streamEnhanced():
    print('StartEnhanced')
    myStreamListener = MyEnhancedStream()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    # myStream.track(languages = ['en'],track=['mariahcarey'],async=True)
    myStream.filter(languages=['en'], track=['mariahcarey'], async=True)


REST_call_left = 900


def restProbe(callLeft):
    print("Starting REST")
    try:
        while(callLeft > 0):
            myrestProbe = api.search(
                q='mariahcarey', languages=['en'], count=100)
            for tweet in myrestProbe:
                data = json.loads(json.dumps(tweet._json))
                db[_COLLECTIONS[0]].insert_one(data)
                db[_COLLECTIONS[3]].insert_one(data)
                print("restDb---inserted")
            callLeft -= 1
        while(callLeft == 0):
            callLeft = api.rate_limit_status
    except TweepError:
        print("TweepError")
    except RateLimitError:
        print("RateLimitError")
        restProbe(0)
    except Exception:
        print(traceback.format_exc())


def streamGeo():
    print("Starting Geo")
    myStreamListener = MyGeoStream()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    # myStream.track(languages = ['en'],track=['mariahcarey'],async=True)
    myStream.filter(languages=['en'], locations=[-4.393201,
                                                 55.781277, -4.071717, 55.929638], async=True)


_RUNNING_TIME = 5 * 60

if __name__ == '__main__':
    # Start as a process
    p = multiprocessing.Process(
        target=streamBasic, name="streamBasic")  # args=(10,))
    p.start()
    p2 = multiprocessing.Process(
        target=streamEnhanced, name="streamEnhanced")  # args=(10,))
    p2.start()
    p3 = multiprocessing.Process(
        target=restProbe, name="restProbe", args=(_RUNNING_TIME,))
    p3.start()
    p4 = multiprocessing.Process(
        target=streamGeo, name="streamGeo")  # args=(10,))
    p4.start()

    p.join(_RUNNING_TIME)
    p2.join(_RUNNING_TIME)
    p3.join(_RUNNING_TIME)
    p4.join(_RUNNING_TIME)

    # If thread is active
    if p.is_alive():
        print("{} {} {}".format("streamBasic has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p.terminate()
        p.join()
    if p2.is_alive():
        print("{} {} {}".format("streamEnhanced has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p2.terminate()
        p2.join()
    if p3.is_alive():
        print("{} {} {}".format("restProbe has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p3.terminate()
        p3.join()
    if p4.is_alive():
        print("{} {} {}".format("streamGeo has been running for ",
                                _RUNNING_TIME, "... let's kill it..."))

        # Terminate foo
        p4.terminate()
        p4.join()
