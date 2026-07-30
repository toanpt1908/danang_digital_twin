# ============================================================
# IMPORT LIBRARIES
# ============================================================

import os
import json
import time
import hashlib
import logging

from datetime import datetime

import requests
import pandas as pd

from bs4 import BeautifulSoup
from tqdm.auto import tqdm



# ============================================================
# CONFIGURATION
# ============================================================

FIRST_PAGE_URL = (

    "https://timkiem.vnexpress.net/"
    "?search_f=title,tag_list"
    "&q=%C4%91%C3%A0%20n%E1%BA%B5ng"
    "&media_type=all"
    "&fromdate=0"
    "&todate=0"
    "&latest="
    "&cate_code=dulich"
    "&date_format=all"

)

SEARCH_URL = (

    "https://timkiem.vnexpress.net/"
    "?q=%C4%91%C3%A0%20n%E1%BA%B5ng"
    "&media_type=all"
    "&fromdate=0"
    "&todate=0"
    "&latest="
    "&cate_code=dulich"
    "&search_f=title,tag_list"
    "&date_format=all"

)

OUTPUT_FILE = "vnexpress_news.csv"

CHECKPOINT_FILE = "vnexpress_checkpoint.json"

SOURCE = "vnexpress"

SOURCE_SYSTEM = "vnexpress_search"

MAX_RETRIES = 3

REQUEST_TIMEOUT = 30

PAGE_DELAY = 1


HEADERS = {

    "User-Agent":

    (

        "Mozilla/5.0 "

        "(Windows NT 10.0; Win64; x64) "

        "AppleWebKit/537.36 "

        "(KHTML, like Gecko) "

        "Chrome/137.0.0.0 "

        "Safari/537.36"

    ),

    "Accept-Language":

    "vi,en-US;q=0.9,en;q=0.8"

}


# ============================================================
# LOGGER
# ============================================================

logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(message)s"

)


# ============================================================
# CREATE SESSION
# ============================================================

def create_session():

    session = requests.Session()

    session.headers.update(

        HEADERS

    )

    return session


# ============================================================
# LOAD OLD CSV
# ============================================================

def load_old_data():

    if not os.path.exists(

        OUTPUT_FILE

    ):

        logging.info(

            "No previous CSV found."

        )

        return pd.DataFrame()

    df = pd.read_csv(
        OUTPUT_FILE,
        parse_dates=["publish_date"]
    )

    logging.info(

        f"Loaded {len(df)} old articles."

    )

    return df


# ============================================================
# LOAD CHECKPOINT
# ============================================================

def load_checkpoint():

    if not os.path.exists(

        CHECKPOINT_FILE

    ):

        logging.info(

            "No checkpoint found."

        )

        return None

    with open(

        CHECKPOINT_FILE,

        "r",

        encoding="utf-8"

    ) as f:

        checkpoint = json.load(f)

    logging.info(

        f"Checkpoint : "

        f"{checkpoint['last_article_url']}"

    )

    return checkpoint


# ============================================================
# SAVE CHECKPOINT
# ============================================================

def save_checkpoint(

    newest_article,

    total_articles

):

    checkpoint = {

        "last_article_id":

            newest_article["article_id"],

        "last_article_url":

            newest_article["url"],

        "last_publish_date":

            newest_article["publish_date"],

        "total_articles":

            total_articles,

        "last_update":

            str(

                pd.Timestamp.now()

            )

    }

    with open(

        CHECKPOINT_FILE,

        "w",

        encoding="utf-8"

    ) as f:

        json.dump(

            checkpoint,

            f,

            ensure_ascii=False,

            indent=4

        )


# ============================================================
# BUILD SEARCH URL
# ============================================================

def build_search_url(page):

    if page == 1:

        return FIRST_PAGE_URL

    return SEARCH_URL + f"&page={page}"


# ============================================================
# DOWNLOAD PAGE
# ============================================================

def get_search_page(

    session,

    page

):

    url = build_search_url(

        page

    )

    for attempt in range(

        MAX_RETRIES

    ):

        try:

            response = session.get(

                url,

                timeout=REQUEST_TIMEOUT

            )

            response.raise_for_status()

            return BeautifulSoup(

                response.text,

                "html.parser"

            )

        except Exception as e:

            logging.warning(

                f"Retry "

                f"{attempt+1}/{MAX_RETRIES}"

            )

            time.sleep(3)

    return None


# ============================================================
# PARSE TIMESTAMP
# ============================================================

def parse_timestamp(

    timestamp

):

    try:

        return datetime.fromtimestamp(

            int(timestamp)

        ).strftime(

            "%Y-%m-%d"

        )

    except Exception:

        return None


# ============================================================
# PARSE ARTICLE
# ============================================================

def parse_article(

    article

):

    title = ""

    summary = ""

    url = ""

    publish_date = None

    title_tag = article.select_one(

        "h3.title-news a"

    )

    if title_tag:

        title = title_tag.get_text(

            strip=True

        )

        url = title_tag.get(

            "href",

            ""

        )

    summary_tag = article.select_one(

        "p.description a"

    )

    if summary_tag:

        summary = summary_tag.get_text(

            " ",

            strip=True

        )

    publish_date = parse_timestamp(

        article.get(

            "data-publishtime"

        )

    )

    return {

        "article_id":

            hashlib.md5(

                url.encode(

                    "utf-8"

                )

            ).hexdigest(),

        "publish_date":

            publish_date,

        "title":

            title,

        "summary":

            summary,

        "url":

            url,

        "source":

            SOURCE,

        "source_system":

            SOURCE_SYSTEM

    }
# ============================================================
# CRAWL NEW ARTICLES
# ============================================================

def crawl_new_articles(

    session,

    checkpoint

):

    new_records = []

    page = 1

    stop_crawling = False

    last_article_url = None

    if checkpoint is not None:

        last_article_url = checkpoint.get(

            "last_article_url"

        )
    MAX_PAGES = 100
    while page <= MAX_PAGES and not stop_crawling:

        logging.info(

            f"Crawling page {page}"

        )

        soup = get_search_page(

            session,

            page

        )

        if soup is None:

            logging.warning(

                f"Cannot load page {page}"

            )

            break

        articles = soup.select(

            "article.item-news.item-news-common"

        )

        if len(articles) == 0:

            logging.info(

                "No more articles."

            )

            break

        logging.info(

            f"Found {len(articles)} articles."

        )

        for article in tqdm(

            articles,

            desc=f"Page {page}"

        ):

            news = parse_article(

                article

            )

            if news["url"] == "":

                continue

            # ---------------------------------------------
            # Stop when reaching checkpoint
            # ---------------------------------------------

            if (

                last_article_url is not None

                and

                news["url"] == last_article_url

            ):

                logging.info(

                    "Checkpoint reached."

                )

                stop_crawling = True

                break

            # ---------------------------------------------
            # Save new article
            # ---------------------------------------------

            new_records.append(

                news

            )

        if stop_crawling:

            break

        page += 1

        time.sleep(

            PAGE_DELAY

        )

    logging.info(

        f"Collected "

        f"{len(new_records)} "

        f"new articles."

    )

    return new_records
# ============================================================
# MERGE OLD DATA + NEW DATA
# ============================================================

def merge_data(

    old_df,

    new_records

):

    if len(new_records) == 0:

        logging.info(

            "No new articles."

        )

        return old_df

    new_df = pd.DataFrame(

        new_records

    )

    if old_df.empty:

        merged_df = new_df.copy()

    else:

        merged_df = pd.concat(

            [

                new_df,

                old_df

            ],

            ignore_index=True

        )

    # --------------------------------------------------------
    # Remove duplicate articles
    # --------------------------------------------------------

    merged_df.drop_duplicates(

        subset="article_id",

        keep="first",

        inplace=True

    )

    # --------------------------------------------------------
    # Convert publish_date
    # --------------------------------------------------------

    merged_df["publish_date"] = pd.to_datetime(

        merged_df["publish_date"],

        errors="coerce"

    )

    # --------------------------------------------------------
    # Sort newest first
    # --------------------------------------------------------

    merged_df.sort_values(

        by="publish_date",

        ascending=False,

        na_position="last",

        inplace=True

    )

    merged_df.reset_index(

        drop=True,

        inplace=True

    )

    # --------------------------------------------------------
    # Crawl Time
    # --------------------------------------------------------

    merged_df["crawl_time"] = pd.Timestamp.now()

    return merged_df


# ============================================================
# SAVE CSV
# ============================================================

def save_csv(

    merged_df

):

    merged_df.to_csv(

        OUTPUT_FILE,

        index=False,

        encoding="utf-8-sig"

    )

    logging.info(

        f"Saved {len(merged_df)} articles."

    )


# ============================================================
# UPDATE CHECKPOINT
# ============================================================

def update_checkpoint(

    merged_df,

    new_records

):

    if len(new_records) == 0:

        logging.info(

            "Checkpoint unchanged."

        )

        return

    newest_article = new_records[0]

    save_checkpoint(

        newest_article=newest_article,

        total_articles=len(

            merged_df

        )

    )

    logging.info(

        "Checkpoint updated."

    )
# ============================================================
# MAIN PIPELINE
# ============================================================

def crawl_vnexpress():

    logging.info("=" * 60)
    logging.info("VNEXPRESS PIPELINE STARTED")
    logging.info("=" * 60)

    start_time = time.time()

    # --------------------------------------------------------
    # Load previous CSV
    # --------------------------------------------------------

    old_df = load_old_data()

    # --------------------------------------------------------
    # Load checkpoint
    # --------------------------------------------------------

    checkpoint = load_checkpoint()

    # --------------------------------------------------------
    # Create Session
    # --------------------------------------------------------

    session = create_session()

    # --------------------------------------------------------
    # Crawl new articles
    # --------------------------------------------------------

    new_records = crawl_new_articles(

        session=session,

        checkpoint=checkpoint

    )

    if len(new_records) == 0:

        logging.info(

            "No new articles found."

        )

        return OUTPUT_FILE

    # --------------------------------------------------------
    # Merge
    # --------------------------------------------------------

    merged_df = merge_data(

        old_df,

        new_records

    )

    # --------------------------------------------------------
    # Save CSV
    # --------------------------------------------------------

    save_csv(

        merged_df

    )

    # --------------------------------------------------------
    # Save Checkpoint
    # --------------------------------------------------------

    update_checkpoint(

        merged_df,

        new_records

    )

    elapsed = time.time() - start_time

    logging.info("=" * 60)
    logging.info("VNEXPRESS PIPELINE FINISHED")
    logging.info("=" * 60)

    logging.info(

        f"Total Articles : {len(merged_df)}"

    )

    logging.info(

        f"New Articles : {len(new_records)}"

    )

    logging.info(

        f"Elapsed : {elapsed/60:.2f} minutes"

    )

    return OUTPUT_FILE


# ============================================================
# RUN LOCALLY
# ============================================================

if __name__ == "__main__":

    crawl_vnexpress()