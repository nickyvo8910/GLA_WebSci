import datetime
import pymongo
from pymongo import MongoClient
import tweepy
from tweepy import TweepError,RateLimitError,StreamListener
import json
import traceback

consumer_key = "VIGGAj16rqqaEOEEJjVWaROpi"
consumer_secret = "HAqUGIRXMvRDkDTdH2fJ1pxsjoRARbepz5rsSeHJWYl2eve1Bl"
access_token = "969038491706363905-VFcCIGYkGRAeG6CPh1kGCPbj8fiHXwo"
access_token_secret = "SkEve79Dkoha1r7wVn34VLyHLendlenfAmemKOpBjxdow"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)#,parser=tweepy.parsers.JSONParser())

mongo = MongoClient('localhost', 27017)
geoDb = mongo['tweet']['geo']

class MyGeoStream(tweepy.StreamListener):
    def on_data(self, data):
        try:
            tweet = json.loads(data)
            geoDb.insert_one(tweet)
            print("geoDb---inserted")
        except TweepError:
            print("TweepError")
        except RateLimitError:
            print("RateLimitError")
        except Exception:
            print(traceback.format_exc())

def _main():
    print("Done Importing & Config")
    print("Starting Geo")
    myStreamListener = MyGeoStream()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    # myStream.track(languages = ['en'],track=['mariahcarey'],async=True)
    myStream.filter(languages = ['en'],locations=[-4.393201,55.781277,-4.071717,55.929638])



_main()
