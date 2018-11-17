import datetime
import pymongo
from pymongo import MongoClient
import tweepy
from tweepy import TweepError,RateLimitError
import json
import traceback


consumer_key = "VIGGAj16rqqaEOEEJjVWaROpi"
consumer_secret = "HAqUGIRXMvRDkDTdH2fJ1pxsjoRARbepz5rsSeHJWYl2eve1Bl"
access_token = "969038491706363905-VFcCIGYkGRAeG6CPh1kGCPbj8fiHXwo"
access_token_secret = "SkEve79Dkoha1r7wVn34VLyHLendlenfAmemKOpBjxdow"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

mongo = MongoClient('localhost', 27017)
db = mongo['tweet']['basic']



class MyStreamListener(tweepy.StreamListener):

    def on_connect(self):
        print("Connected")

    def on_status(self, status):
        print(status.text).encode("utf-8")

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            db.insert_one(tweet)
            print("inserted")
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())

def _main():
    print("Done Importing & Config")


    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

    myStream.sample(languages = ['en'])

_main()
