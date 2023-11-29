import getopt
import sys
import os
import bz2
import json
from datetime import datetime
import networkx as nx
import argparse
from mpi4py import MPI
import time
from itertools import combinations

//grupo yenelis molina ,freddy fajardo y emanuel escobar

def process_tweets_mpi(directory, start_date, end_date, hashtags):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()


    all_files = [os.path.join(root, file) for root, dirs, files in os.walk(directory) for file in files if file.endswith('.json.bz2')]
    files_per_process = len(all_files) // size
    start_index = rank * files_per_process
    end_index = None if rank == size - 1 else start_index + files_per_process
    my_files = all_files[start_index:end_index]


    tweets = []
    for file_path in my_files:
        with bz2.open(file_path, 'rt') as bzfile:
            for line in bzfile:
                tweet = json.loads(line)
                if is_tweet_valid(tweet, start_date, end_date, hashtags):
                    tweets.append(tweet)


    all_tweets = comm.gather(tweets, root=0)


    if rank == 0:
        combined_tweets = [tweet for process_tweets in all_tweets for tweet in process_tweets]
        return combined_tweets
    else:
        return []

def is_tweet_valid(tweet, start_date, end_date, hashtags):
    if 'created_at' not in tweet:
        return False
    tweet_date = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    if start_date and tweet_date < datetime.combine(start_date, datetime.min.time()):
        return False
    if end_date and tweet_date > datetime.combine(end_date, datetime.max.time()):
        return False
    tweet_hashtags = {hashtag_item['text'].lower() for hashtag_item in tweet['entities']['hashtags']}
    if hashtags and not hashtags.intersection(tweet_hashtags):
        return False
    return True

def process_tweet(tweet, retweet_dict):
    retweeter = tweet['user']['screen_name']
    if 'retweeted_status' in tweet:
        author = tweet['retweeted_status']['user']['screen_name']
        if author != retweeter and author != "null" and retweeter != "null":
            retweet_dict.setdefault(author, set()).add(retweeter)

def update_retweet_dict(tweet, retweet_dict):
    retweeter = tweet['user']['screen_name']
    if 'retweeted_status' in tweet:
        author = tweet['retweeted_status']['user']['screen_name']
        if author != retweeter and author != "null" and retweeter != "null":
            retweet_dict.setdefault(retweeter, []).append(author)

def process_combinations(authors_list, retweeter, coretweets_data):
    seen_authors = set()
    for author in authors_list:
        if author not in seen_authors and author != retweeter:
            seen_authors.add(author)

    for combo in combinations(seen_authors, 2):
        combo_key = tuple(sorted(combo))
        if combo_key not in coretweets_data:
            coretweets_data[combo_key] = {'authors': {'u1': combo_key[0], 'u2': combo_key[1]}, 'totalCoretweets': 0, 'retweeters': []}
        coretweets_data[combo_key]['totalCoretweets'] += 1
        if retweeter not in coretweets_data[combo_key]['retweeters']:
            coretweets_data[combo_key]['retweeters'].append(retweeter)

def create_coretweet_json(tweets):
    retweet_dict = {}
    coretweets_data = {}

    for tweet in tweets:
        update_retweet_dict(tweet, retweet_dict)

    for retweeter, authors_list in retweet_dict.items():
        process_combinations(authors_list, retweeter, coretweets_data)

    sorted_coretweets = sorted(coretweets_data.values(), key=lambda x: x['totalCoretweets'], reverse=True)
    return {'coretweets': sorted_coretweets}

def update_mention_json_data(mentions_data, user_screen_name, mentioned_user, tweet_id):
    if mentioned_user not in mentions_data:
        mentions_data[mentioned_user] = {
            "username": mentioned_user,
            "receivedMentions": 0,
            "mentions": []
        }

    existing_mention = None
    for mention in mentions_data[mentioned_user]["mentions"]:
        if mention["mentionBy"] == user_screen_name:
            existing_mention = mention
            break

    if not existing_mention:
        mentions_data[mentioned_user]["mentions"].append({
            "mentionBy": user_screen_name,
            "tweets": [tweet_id]
        })
    else:
        existing_mention["tweets"].append(tweet_id)

    mentions_data[mentioned_user]["receivedMentions"] += 1





def save_graph(data, filename):
    nx.write_gexf(data, filename)

def save_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def process_retweet_for_json(tweet, retweet_data):
    if 'retweeted_status' in tweet:
        original_tweet_id = tweet['retweeted_status']['id_str']
        retweeter = tweet['user']['screen_name']
        original_author = tweet['retweeted_status']['user']['screen_name']

        if original_author not in retweet_data:
            retweet_data[original_author] = {"receivedRetweets": 0, "tweets": {}}

        tweet_entry = retweet_data[original_author]["tweets"].setdefault(f"tweetId: {original_tweet_id}", {"retweetedBy": []})
        
        if retweeter not in tweet_entry["retweetedBy"]:
            tweet_entry["retweetedBy"].append(retweeter)
            retweet_data[original_author]["receivedRetweets"] += 1


def create_rt_json(tweets):
    retweet_data = {}

    for tweet in tweets:
        process_retweet_for_json(tweet, retweet_data)
    sorted_retweets = sorted(retweet_data.items(), key=lambda x: x[1]["receivedRetweets"], reverse=True)

    retweets_list = [{"username": username, "receivedRetweets": data["receivedRetweets"], "tweets": data["tweets"]} for username, data in sorted_retweets]
    
    return {"retweets": retweets_list}



def create_corretweet_graph(tweets):
    G = nx.Graph()
    
    retweet_dict = {}
    for tweet in tweets:
        update_retweet_dict(tweet, retweet_dict)
    
    for retweeter, authors_list in retweet_dict.items():
        for combo in combinations(authors_list, 2):
            author1, author2 = sorted(combo)
            G.add_edge(author1, author2)

    return G


def create_retweet_graph(tweets):
    G = nx.DiGraph()

    for tweet in tweets:
        if 'retweeted_status' in tweet:
            original_author = tweet['retweeted_status']['user']['screen_name']
            retweeter = tweet['user']['screen_name']
            if original_author != retweeter and original_author != "null" and retweeter != "null":
                G.add_edge(original_author, retweeter)

    return G

def create_mentions_json(tweets):
    mentions_data = {}

    for tweet in tweets:
        user_screen_name = tweet["user"]["screen_name"] if 'user' in tweet else None
        retweeted = "retweeted_status" in tweet

        if not retweeted and user_screen_name:
            mentioned_users = {mention["screen_name"] for mention in tweet.get("entities", {}).get("user_mentions", []) if mention["screen_name"] != "null"}
            
            for mentioned_user in mentioned_users:
                update_mention_json_data(mentions_data, user_screen_name, mentioned_user, tweet["id_str"])

    sorted_mentions = sorted(mentions_data.values(), key=lambda x: x['receivedMentions'], reverse=True)
    return {'mentions': sorted_mentions}

def create_mentions_graph(tweets):
    G = nx.DiGraph()

    for tweet in tweets:
        user_screen_name = tweet.get("user", {}).get("screen_name", "null")
        mentioned_users = {mention["screen_name"] for mention in tweet.get("entities", {}).get("user_mentions", []) if mention["screen_name"] != "null"}

        for mentioned_user in mentioned_users:
            if user_screen_name != mentioned_user:
                G.add_edge(user_screen_name, mentioned_user)

    return G



def sort_data(data, sort_key):
    return dict(sorted(data.items(), key=lambda item: item[1][sort_key], reverse=True))

def process_output(args, tweets):
    if args.grt:
        rt_graph = create_retweet_graph(tweets)
        save_graph(rt_graph, "rtp.gexf")

    if args.gm:
        mention_graph = create_mentions_graph(tweets)
        save_graph(mention_graph, "menciónp.gexf")

    if args.gcrt:
        coretweet_graph = create_corretweet_graph(tweets)
        save_graph(coretweet_graph, "corrtwp.gexf")
    
    if args.jm:
        mention_json = create_mentions_json(tweets)
        save_json("menciónp.json", mention_json)

    if args.jrt:
        rt_json = create_rt_json(tweets)
        save_json("rtp.json",rt_json, )

    if args.jcrt:
        coretweet_json = create_coretweet_json(tweets)
        save_json( "corrtwp.json",coretweet_json)

def main():
    parser = argparse.ArgumentParser(description='Input ARGS error',  add_help=False)
    
    parser.add_argument('-d', '--directory', type=str, default='data', help='Directorio relativo donde buscar los tweets')
    parser.add_argument('-fi', '--start_date', type=lambda s: datetime.strptime(s, '%d-%m-%y'), help='Fecha inicial en formato dd-mm-aa')
    parser.add_argument('-ff', '--end_date', type=lambda s: datetime.strptime(s, '%d-%m-%y'), help='Fecha final en formato dd-mm-aa')
    parser.add_argument('-h', '--hashtag_file', type=str, help='Archivo de texto con hashtags para filtrar los tweets')
    parser.add_argument("-grt", action="store_true", help="Create retweet graph")
    parser.add_argument("-jrt", action="store_true", help="Create retweet JSON")
    parser.add_argument("-gm", action="store_true", help="Create mention graph")
    parser.add_argument("-jm", action="store_true", help="Create mention JSON")
    parser.add_argument("-gcrt", action="store_true", help="Create co-retweet graph")
    parser.add_argument("-jcrt", action="store_true", help="Create co-retweet JSON")
    args = parser.parse_args()
    hashtags = set()
    if args.hashtag_file:
        with open(args.hashtag_file, 'r') as file:
            hashtags = {line.strip().lower() for line in file}
    validate_args(args)
    tweets = process_tweets_mpi(args.directory, args.start_date, args.end_date, hashtags)
    process_output(args, tweets)

def validate_args(args):
    if isinstance(args.start_date, str):
        args.start_date = datetime.strptime(args.start_date, "%d-%m-%y")
    elif args.start_date is None:
        args.start_date = datetime.strptime("01-01-00", "%d-%m-%y")

    if isinstance(args.end_date, str):
        args.end_date = datetime.strptime(args.end_date, "%d-%m-%y")
    elif args.end_date is None:
        args.end_date = datetime.strptime("01-01-29", "%d-%m-%y")
if __name__ == "__main__":
    start_time = time.time()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    main()
    if rank == 0:
        print(f"{time.time() - start_time:.2f}")
