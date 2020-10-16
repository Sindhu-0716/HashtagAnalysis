import json
from datetime import datetime

import pymongo as pymongo
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


client = pymongo.MongoClient("mongodb+srv://user:a4oHIpvaQdrftEAp@cluster0.czyuu.gcp.mongodb.net/hackathon?retryWrites=true&w=majority")
db = client.hackathon


# Enter Twitter API Keys
access_token = "488650850-SdUNecgQyDGo7n5Rl85hlFKb51znw6fQZD8LH1nV"
access_token_secret = "wyShJScbtuDjfAeKQ6MCOufuEEhvxPYGPeLBGs6olRlbk"
consumer_key = "YV34hSKe6wXoKp3qSekqtGtMR"
consumer_secret = "QIbpHiRTmAAO2sfNsz49YeCz4a311QY8UY2yTpM75K8BEkELn3"

file_name = int(datetime.utcnow().timestamp() * 1e3)


# Create the class that will handle the tweet stream
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)


class listener(StreamListener):
    def on_data(self, data):
        try:
            tweet = json.loads(data)
            # if tweet.get('place') and tweet.get('place').get('country') and tweet['place']['country'] == "United States":
            if 'covid' in tweet.get('text').lower() or 'coronavirus' in tweet.get('text').lower() or 'plandemic ' in tweet.get('text').lower() or 'pandemic ' in tweet.get('text').lower():
                convertedTweet = {}
                convertedTweet['created_at'] = tweet['created_at']
                convertedTweet['text'] = tweet['text'].lower()
                convertedTweet['user'] = {}
                convertedTweet['user']['location'] = tweet['user']['location']
                convertedTweet['geo'] = tweet['geo']
                convertedTweet['coordinates'] = tweet['coordinates']
                if tweet.get('place'):
                    convertedTweet['place'] = {}
                    convertedTweet['place']['place_type'] = tweet['place']['place_type']
                    convertedTweet['place']['name'] = tweet['place']['name']
                    convertedTweet['place']['full_name'] = tweet['place']['full_name']
                    convertedTweet['place']['country'] = tweet['place']['country']
                    convertedTweet['place']['bounding_box'] = {}
                    convertedTweet['place']['bounding_box']["type"] = tweet['place']['bounding_box']["type"]
                    convertedTweet['place']['bounding_box']["coordinates"] = tweet['place']['bounding_box']["coordinates"]
                convertedTweet['reply_count'] = tweet['reply_count']
                convertedTweet['retweet_count'] = tweet['retweet_count']
                hasttagArray = []
                if tweet.get('entities'):
                    if tweet['entities'].get('hashtags'):
                        for item in tweet['entities']['hashtags']:
                            hasttagArray.append(item['text'])
                convertedTweet['hashtags'] = hasttagArray

                print("data:")
                print(data)
                print("convertedTweet:")
                print(json.dumps(convertedTweet))

                db.inventory.insert_one(convertedTweet)
            # print(json.dumps(tweet))

        except BaseException:
            print('Error')
            pass

    # def on_status(self, status):
    #     print
    #     status.text
    #     if status.coordinates:
    #         print
    #         'coords:', status.coordinates
    #     if status.place:
    #         print
    #         'place:', status.place.full_name
    #
    #     return True
    #
    # on_event = on_status

    def on_error(self, status):
        print(status)


my_listener = listener()

if __name__ == '__main__':
    # Handle Twitter authetification and the connection to Twitter Streaming API
    # track = ['#covid19','#covid2019','#coronavirususa','#coronaapocolypse','#coronaday','#coronavirusu','#covid2019pt','#COVID19PT','#codvid_19','#codvid19','corona','corona vairus','#covid','#coronavirusoutbreak','#coronaviruspandemic','#coronavirusupdates','#covid_19']
    location = [-125.3, 24.8, -63.7, 49.1]

    l = StdOutListener()
    json_listener = listener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, json_listener, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    stream.filter(locations=location, filter_level="low")
