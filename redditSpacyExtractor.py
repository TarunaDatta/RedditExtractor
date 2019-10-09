import math
import requests
import itertools
import numpy as np
import praw
import time
from datetime import datetime, timedelta

import spacy


class RedditSpacyExtractor:
    def __init__(self,
                 subreddit=None,
                 min_score=5,
                 min_sent_len=5,
                 max_sent_len=30,
                 author=None,
                 before=None,
                 after=None,
                 size=32,
                 max_retries=5):
        self.subreddit = subreddit
        self.min_score = min_score
        self.min_sent_len = min_sent_len
        self.max_sent_len = max_sent_len
        self.author = author
        self.before = before
        self.after = after
        self.size = size
        self.max_retries = max_retries
        self.reddit = praw.Reddit("scraper1")
        self.nlp = spacy.load("en")

    def URI_from_fields(self, subreddit, author, before, after, size):
        URI_TEMPLATE = "https://api.pushshift.io/reddit/search/comment/?"

        and_flag = False
        if subreddit:
            if and_flag:
                URI_TEMPLATE += "&"
            URI_TEMPLATE += "subreddit=%s" % subreddit
            and_flag = True

        if author:
            if and_flag:
                URI_TEMPLATE += "&"
            URI_TEMPLATE += "author=%s" % author
            and_flag = True
            
        if before:
            if and_flag:
                URI_TEMPLATE += "&" 
            URI_TEMPLATE += "before=%s" % before
            and_flag = True
            
        if after:
            if and_flag:
                URI_TEMPLATE += "&"
            URI_TEMPLATE += "after=%s" % after
            and_flag = True
            
        if size:
            if and_flag:
                URI_TEMPLATE += "&"
            URI_TEMPLATE += "size=%s" % size
            and_flag = True
            
        return URI_TEMPLATE

    def make_request(self, uri):
        def fire_away(uri):
            print(uri)
            response = requests.get(uri)
            assert response.status_code == 200
            return response.json()

        current_tries = 1
        while current_tries < self.max_retries:
            try:
                print("sleeping")
                time.sleep(1)
                response = fire_away(uri)
                return response
            except:
                time.sleep(1)
                current_tries += 1
        return fire_away(uri)

    def retrieve_json(self, start_from=None, count=500):
        uri = self.URI_from_fields(
                            subreddit=self.subreddit,
                            author=self.author,
                            before=self.before,
                            after=start_from,
                            size=count)
        more_posts = self.make_request(uri)["data"]
        return more_posts
    
    def get_comments(self):
        post_collections = []
        collection_size = 0
        if self.after is not None:
            new_start_at = self.after
        else:
            new_start_at = math.floor((datetime.utcnow() -
                                       timedelta(days=365)).timestamp())
        while collection_size != self.size:
            print("starting from :", new_start_at,
                  "post_collections length :", collection_size)
            retrieved = self.retrieve_json(new_start_at, min(500, self.size - collection_size))
            post_collections += retrieved
            if collection_size != 0:
                last = post_collections[-1]
                new_start_at = last["created_utc"] - (10)
            else:
                new_start_at -= 10
            

        return post_collections

    def validCommentFilter(self, comment):
        if comment["body"] == "[removed]":
            return False
        if comment["author"] == "[deleted]":
            return False
        if comment["body"] == "[removed]":
            return False
        if self.reddit.comment(comment["id"]).score < self.min_score:
            return False
        doc = self.nlp(comment["body"])
        sents = [sent for sent in doc.sents if len(sent) <= self.max_sent_len]
        sents = [sent for sent in doc.sents if len(sent) >= self.min_sent_len]
        # sents = [sent for sent in doc.sents if "\n" not in "".join([tok.text for tok in sent])]
        # sents = [[token for token in sent] for sent in sents]
        if len(sents) > 0:
            return True
        
        
        
        
            
            
