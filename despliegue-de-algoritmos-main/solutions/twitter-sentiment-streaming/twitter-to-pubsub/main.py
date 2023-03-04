import os

from google.cloud import pubsub_v1
from tweepy import OAuthHandler, Stream, API
from tweepy.streaming import StreamListener
from loguru import logger

# Get your twitter credentials from the environment variables.
CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACESSS_TOKEN_SECRET = os.getenv("ACESSS_TOKEN_SECRET")

PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_NAME = os.getenv("TOPIC_NAME")

TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"

publisher = pubsub_v1.PublisherClient()


class MyStreamListener(StreamListener):

    count = 0
    total_tweets = 100

    def publish_tweet(self, text):
        publisher.publish(TOPIC_PATH, str.encode(text))

    def on_status(self, status):

        keep_going = True

        if hasattr(status, "retweeted_status"):  # Check if Retweet
            try:
                text = status.retweeted_status.extended_tweet["full_text"]
            except AttributeError:
                text = status.retweeted_status.text
        else:
            try:
                text = status.extended_tweet["full_text"]
            except AttributeError:
                text = status.text

        logger.info(text)
        self.publish_tweet(text)

        self.count += 1
        if self.count > self.total_tweets:
            keep_going = False
        return keep_going

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False


def main(request):

    topic_name = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"
    # publisher.create_topic(topic_name)

    logger.info("Creating OAuth...")
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACESSS_TOKEN_SECRET)
    api = API(auth)

    logger.info("Starting listener...")
    myStreamListener = MyStreamListener()
    myStream = Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(track=["trump"])
