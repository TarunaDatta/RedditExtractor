import math
import requests
import collections
import praw
import time
from datetime import datetime, timedelta
import string
import pickle
import os
import json
# import re
import spacy
import argparse
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk import download





class RedditSpacyExtractor:
    def __init__(self,
                 subreddit=None,
                 min_score=4,
                 min_len=8,
                 min_sent_len=6,
                 max_sent_len=32,
                 author=None,
                 before=None,
                 size=32,
                 max_retries=5,
                 counter_file="vocab_counter.pkl",
                 collectionp_file="collection.pkl",
                 sentence_file="sentences.txt",
                 mode=all,
                 spacy_use=False):
        if mode == "userwise":
            self.mode = "userwise"
            self.users = dict()
        else:
            self.mode = "all"
            self.users = None
        self.subreddit = subreddit
        self.min_score = min_score
        self.min_len = min_len
        self.min_sent_len = min_sent_len
        self.max_sent_len = max_sent_len
        self.author = author
        self.before = before
        self.size = size
        self.max_retries = max_retries
        # a praw.ini file is needed in the directory for this scraper to work
        self.reddit = praw.Reddit("scraper1")
        if spacy_use:
            spacy.prefer_gpu()
            self.nlp = spacy.load("en")
        else:
            download('punkt')
        self.spacy_use = spacy_use
        self.counter = collections.Counter()
        self.commentids = []
        self.after = None
        
        self.collectionp_file = collectionp_file
        self.counter_file = counter_file
        self.sentence_file = sentence_file

        self.sent_count = 0

        if os.path.isfile(self.collectionp_file):
            self.collectionp = pickle.load(open(self.collectionp_file, "rb"))
            self.commentids = self.collectionp["commentids"]
            self.after = self.collectionp["after"]
            self.sent_count = self.collectionp["sentence_count"]

        if os.path.isfile(self.counter_file):
            self.counter = pickle.load(open(self.counter_file, "rb"))

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
            
        if after:
            if and_flag:
                URI_TEMPLATE += "&"
            URI_TEMPLATE += "after=%d" % after
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
        print("starting from :", self.after,
              "post_collections length :", self.sent_count)
        retrieve_size = min(self.size, 500)
        retrieved = self.retrieve_json(self.after, retrieve_size)
        return retrieved

    def validCommentFilter(self, comment):
        if comment["body"] == "[removed]":
            return False
        elif comment["author"] == "[deleted]":
            return False
        elif comment["body"] == "[removed]":
            return False
        elif len(comment["body"]) < self.min_len:
            return False
        elif self.reddit.comment(comment["id"]).score < self.min_score:
            return False
        elif "Bot" in comment["body"] or "Github" in comment["body"] or ".com" in comment["body"] or ".org" in comment["body"]:
            return False
        else:
            return True

    def commentCleaner(self, comment):
        sents = sent_tokenize(comment["body"])
        sents = [[token for token in word_tokenize(sent)
                  if token not in string.punctuation]
                 for sent in sents]
        sents = [sent for sent in sents
                 if len(sent) >= self.min_sent_len
                 and len(sent) <= self.max_sent_len]
        for sent in sents:
            for word in sent:
                self.counter[word] += 1
        sents = [" ".join(sent) for sent in sents]
        return sents

    def spacyCleaner(self, comment):
        doc = self.nlp(comment["body"])
        sents = list(doc.sents)
        sents = [[token.text for token in sent
                  if token.text not in string.punctuation]
                 for sent in sents]
        for sent in sents:
            for word in sent:
                self.counter[word] += 1
        sents = [" ".join(sent) for sent in sents]
        return sents

    def get_sentences(self):
        if self.before:
            self.after = math.floor((self.before -
                                     timedelta(days=365)).timestamp())
        else:
            self.after = math.floor((datetime.now() -
                                     timedelta(days=365)).timestamp())
        while self.sent_count < self.size:
            print("Retrieving comments")
            comments = self.get_comments()
            print("Filtering comments")
            comments = [comment for comment in comments
                        if self.validCommentFilter(comment)
                        and comment["id"] not in self.commentids]
            self.commentids += [comment["id"] for comment in comments]

            if len(comments) != 0:
                last = comments[-1]
                self.after = last["created_utc"] - (10000)
            else:
                self.after -= 10000

            print("Saving collectionp")
            with open(self.collectionp_file, "wb") as pklfile:
                self.collectionp = {"after": self.after,
                                    "commentids": self.commentids,
                                    "sentence_count": self.sent_count}
                pickle.dump(self.collectionp, pklfile)

            print("Saving counter")
            with open(self.counter_file, "wb") as pklfile:
                pickle.dump(self.counter, pklfile)

            if self.mode == "all":
                print("Cleaning sentences")
                if self.spacy_use:
                    print("Using Spacy")
                    cleaned_sents = [self.commentCleaner(comment)
                                     for comment in comments]
                else:
                    print("Using NLTK")
                    cleaned_sents = [self.commentCleaner(comment)
                                     for comment in comments]

                sents = [sent for sent in cleaned_sents if len(sent) != 0]
                with open(self.sentence_file, "a+") as sent_file:
                    for line in sents:
                        for sent in line:
                            sent_file.write(sent + "\n")
                            self.sent_count += 1

            elif self.mode == "userwise":
                print("Cleaning sentences userwise")
                if self.spacy_use:
                    print("Using Spacy")
                    for comment in comments:
                        if comment["author"] not in self.users:
                            self.users[comment["author"]] = self.commentCleaner(comment)
                        else:
                            self.users[comment["author"]] += self.commentCleaner(comment)
                else:
                    print("Using NLTK")
                    for comment in comments:
                        if comment["author"] not in self.users:
                            self.users[comment["author"]] = self.commentCleaner(comment)
                        else:
                            self.users[comment["author"]] += self.commentCleaner(comment)

                delkeys  = []
                for key in self.users.keys():
                    self.users[key] = [sent for sent in self.users[key] if len(sent) != 0]
                    if len(self.users[key]) == 0:
                        delkeys.append(key)

                for key in delkeys:
                    del(self.users[key])
                        

                self.sent_count = sum([len(sents) for sents in self.users.values()])

                print(self.users)
                with open(self.sentence_file, "w") as sent_file:
                    json.dump(self.users, sent_file)
                    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extractor options")
    parser.add_argument("--subreddit", type=str, default=None,
                        help="subreddit from which to extract")
    parser.add_argument("--min_score", type=int, default=4,
                        help="minimum score for comment to be extracted")
    parser.add_argument("--min_len", type=int, default=8,
                        help="minimum number of characters in the comment, needed to filter one word comments and such")
    parser.add_argument("--min_sent_len", type=int, default=6,
                        help="minimum number of words in the sentence")
    parser.add_argument("--max_sent_len", type=int, default=32,
                        help="maximum number of words in the sentence")
    parser.add_argument("--author", type=str, default=None,
                        help="filter by specific author")
    parser.add_argument("--before", type=str, default=None,
                        help="timestamp that signifies end point of extraction window")
    parser.add_argument("--size", type=int, default=32,
                        help="Number of comments to be extracted")
    parser.add_argument("--max_retries", type=int, default=5,
                        help="Maximum number of retries before giving up")
    parser.add_argument("--counter_file", type=str, default="vocab_counter.pkl",
                        help="The pickle file containing counter information")
    parser.add_argument("--mode", type=str, default="all",
                        help="The mode in which to run the collection process")
    parser.add_argument("--collectionp_file", type=str, default="collection.pkl",
                        help="Pickle file containing collection progress")
    parser.add_argument("--sentence_file", type=str, default="sentences.txt",
                        help="File name for the output file")
    parser.add_argument("--spacy_use", type=bool, default=False,
                        help="Whether or not to use spacy")
    args = parser.parse_args()
    print(args)
    ex = RedditSpacyExtractor(subreddit=args.subreddit,
                              min_score=args.min_score,
                              min_len=args.min_len,
                              min_sent_len=args.min_sent_len,
                              max_sent_len=args.max_sent_len,
                              author=args.author,
                              before=args.before,
                              size=args.size,
                              max_retries=args.max_retries,
                              counter_file=args.counter_file,
                              collectionp_file=args.collectionp_file,
                              sentence_file=args.sentence_file,
                              spacy_use=args.spacy_use,
                              mode=args.mode)
    ex.get_sentences()
