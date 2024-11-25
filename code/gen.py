import json
import logging
import time

import requests
from tqdm import tqdm
from newspaper import Article

from main import init_config
from util.util import DataCollector
from util.util import Config, create_dir
from util import Constants
import csv

import sys


def crawl_link_article(url):
    result_json = None

    try:
        if "http" not in url:
            if url[0] == "/":
                url = url[1:]
            try:
                article = Article("http://" + url)
                article.download()
                time.sleep(2)
                article.parse()
                flag = True
            except:
                logging.exception("Exception in getting data from url {}".format(url))
                flag = False
                pass
            if flag == False:
                try:
                    article = Article("https://" + url)
                    article.download()
                    time.sleep(2)
                    article.parse()
                    flag = True
                except:
                    logging.exception(
                        "Exception in getting data from url {}".format(url)
                    )
                    flag = False
                    pass
            if flag == False:
                return None
        else:
            try:
                article = Article(url)
                article.download()
                time.sleep(2)
                article.parse()
            except:
                logging.exception("Exception in getting data from url {}".format(url))
                return None

        if not article.is_parsed:
            return None

        visible_text = article.text
        top_image = article.top_image
        images = article.images
        keywords = article.keywords
        authors = article.authors
        canonical_link = article.canonical_link
        title = article.title
        meta_data = article.meta_data
        movies = article.movies
        publish_date = article.publish_date
        source = article.source_url
        summary = article.summary

        result_json = {
            "url": url,
            "text": visible_text,
            "images": list(images),
            "top_img": top_image,
            "keywords": keywords,
            "authors": authors,
            "canonical_link": canonical_link,
            "title": title,
            "meta_data": meta_data,
            "movies": movies,
            "publish_date": get_epoch_time(publish_date),
            "source": source,
            "summary": summary,
        }
    except:
        logging.exception("Exception in fetching article form URL : {}".format(url))

    return result_json


def get_epoch_time(time_obj):
    if time_obj:
        return time_obj.timestamp()

    return None


def get_web_archieve_results(search_url):
    try:
        archieve_url = (
            "http://web.archive.org/cdx/search/cdx?url={}&output=json".format(
                search_url
            )
        )

        response = requests.get(archieve_url)
        response_json = json.loads(response.content)

        response_json = response_json[1:]

        return response_json

    except:
        return None


def get_website_url_from_arhieve(url):
    """Get the url from http://web.archive.org/ for the passed url if exists."""
    archieve_results = get_web_archieve_results(url)
    if archieve_results:
        modified_url = "https://web.archive.org/web/{}/{}".format(
            archieve_results[0][1], archieve_results[0][2]
        )
        return modified_url
    else:
        return None


def crawl_news_article(url):
    news_article = crawl_link_article(url)

    # If the news article could not be fetched from original website, fetch from archieve if it exists.
    if news_article is None:
        archieve_url = get_website_url_from_arhieve(url)
        if archieve_url is not None:
            news_article = crawl_link_article(archieve_url)

    return news_article


def collect_news_articles(news_list, news_source, label, config: Config):
    create_dir("./out")
    with open("./out/{}_{}.csv".format(news_source, label), "w", newline="") as f:

        w = csv.DictWriter(f, ["id", "news_url", "title", "H", "tweet_ids"])
        w.writeheader()
        for news in tqdm(news_list):
            news_article = crawl_news_article(news["news_url"])
            h = "none/not available"
            if news_article and news_article["text"]:
                h = news_article["text"]
            news["H"] = h
            w.writerow(news)


class NewsContentCollector(DataCollector):

    def __init__(self, config):
        super(NewsContentCollector, self).__init__(config)

    def load_news_file(self, data_choice):
        maxInt = sys.maxsize
        while True:
            # decrease the maxInt value by factor 10
            # as long as the OverflowError occurs.
            try:
                csv.field_size_limit(maxInt)
                break
            except OverflowError:
                maxInt = int(maxInt / 10)

        news_list = []
        with open(
            "{}/{}_{}.csv".format(
                self.config.dataset_dir,
                data_choice["news_source"],
                data_choice["label"],
            ),
            encoding="UTF-8",
        ) as csvfile:
            reader = csv.DictReader(csvfile)
            for news in reader:
                news_list.append(news)

        return news_list

    def collect_data(self, choices):
        for choice in choices:
            news_list = self.load_news_file(choice)
            collect_news_articles(
                news_list, choice["news_source"], choice["label"], self.config
            )


def init_logging(config):
    format = "%(asctime)s %(process)d %(module)s %(levelname)s %(message)s"
    # format = '%(message)s'
    logging.basicConfig(
        filename="data_collection_{}.log".format(str(int(time.time()))),
        level=logging.INFO,
        format=format,
    )
    logging.getLogger("requests").setLevel(logging.CRITICAL)


def download_dataset():
    config, data_choices, data_features_to_collect = init_config()
    init_logging(config)
    data_collector = NewsContentCollector(config)
    data_collector.collect_data(data_choices)


if __name__ == "__main__":
    download_dataset()
