# ============================================================
# IMPORT LIBRARIES
# ============================================================

import os
import json
import time
import logging
import hashlib

from datetime import datetime

import requests
import pandas as pd

from tqdm.auto import tqdm


# ============================================================
# CONFIGURATION
# ============================================================

BASE_API = "https://baodanang.vn/api/getMoreArticle"

FIRST_CURSOR = 3333125

LIMIT = 450

OFFSET = 0

SOURCE = "baodanang"

SOURCE_SYSTEM = "baodanang_api"

OUTPUT_FILE = "baodanang_news.csv"

CHECKPOINT_FILE = "baodanang_checkpoint.json"

REQUEST_TIMEOUT = 30

REQUEST_DELAY = 1

MAX_RETRIES = 3

KEYWORDS = [

    "đà nẵng",

    "bà nà",

    "bà nà hills",

    "sơn trà",

    "mỹ khê",

    "ngũ hành sơn",

    "cầu rồng",

    "hòa vang",

    "da nang",

    "danang"

]


HEADERS = {

    "User-Agent":

        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0 Safari/537.36",

    "Accept":

        "application/json, text/plain, */*",

    "Referer":

        "https://baodanang.vn/du-lich"

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
# LOAD OLD DATA
# ============================================================

def load_old_data():

    if not os.path.exists(

        OUTPUT_FILE

    ):

        logging.info(

            "No previous CSV found."

        )

        return pd.DataFrame()

    try:

        df = pd.read_csv(

            OUTPUT_FILE,

            parse_dates=[

                "publish_date"

            ]

        )

        logging.info(

            f"Loaded {len(df)} old articles."

        )

        return df

    except Exception as e:

        logging.warning(

            f"Cannot load CSV: {e}"

        )

        return pd.DataFrame()


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

        checkpoint.get(

            "last_article_url",

            "None"

        )

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

            pd.to_datetime(

                newest_article["publish_date"]

            ).strftime(

                "%Y-%m-%d"

            ),

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
# BUILD API URL
# ============================================================

def build_api_url(cursor):

    return (

        f"{BASE_API}/"
        f"channel_empty_{cursor}_{LIMIT}_{OFFSET}"

    )


# ============================================================
# DOWNLOAD JSON
# ============================================================

def get_articles(

    session,

    cursor

):

    url = build_api_url(

        cursor

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

            data = response.json()

            if not isinstance(

                data,

                list

            ):

                return []

            return data

        except Exception as e:

            logging.warning(

                f"Retry {attempt+1}/{MAX_RETRIES}"

            )

            logging.warning(

                str(e)

            )

            time.sleep(3)

    return []


# ============================================================
# PARSE ARTICLE
# ============================================================

def parse_article(item):

    try:

        title = (

            item.get(

                "Title",

                ""

            )

            .strip()

        )

        summary = (

            item.get(

                "Headlines",

                ""

            )

            .strip()

        )

        url = item.get(

            "LinktoMe2",

            ""

        ).strip()

        publish_date = (

            item.get(

                "Time_yyyyMMddHHmmss",

                ""

            )[:10]

        )

        article_id = str(

            item.get(

                "PublisherId"

            )

        )

        # ----------------------------------------------------
        # Keyword Filter
        # ----------------------------------------------------

        content = (

            title

            + " "

            + summary

        ).lower()

        if not any(

            keyword in content

            for keyword in KEYWORDS

        ):

            return None

        return {

            "article_id":

                article_id,

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

    except Exception as e:

        logging.warning(

            f"Parse error: {e}"

        )

        return None
# ============================================================
# CRAWL NEW ARTICLES
# ============================================================

def crawl_new_articles(

    session,

    checkpoint

):

    new_records = []

    seen_urls = set()

    stop_crawling = False

    cursor = FIRST_CURSOR

    last_article_url = None

    if checkpoint is not None:

        last_article_url = checkpoint.get(

            "last_article_url"

        )

    while not stop_crawling:

        logging.info(

            f"Cursor : {cursor}"

        )

        articles = get_articles(

            session,

            cursor

        )

        if len(articles) == 0:

            logging.info(

                "No more articles."

            )

            break

        logging.info(

            f"Found {len(articles)} articles."

        )

        for item in tqdm(

            articles,

            desc=f"Cursor {cursor}"

        ):

            news = parse_article(

                item

            )

            if news is None:

                continue

            # ------------------------------------------------
            # Reached checkpoint
            # ------------------------------------------------

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

            # ------------------------------------------------
            # Duplicate
            # ------------------------------------------------

            if news["url"] in seen_urls:

                continue

            seen_urls.add(

                news["url"]

            )

            new_records.append(

                news

            )

        if stop_crawling:

            break

        # ----------------------------------------------------
        # Cursor Pagination
        # ----------------------------------------------------

        cursor = articles[-1]["PublisherId"]

        time.sleep(

            REQUEST_DELAY

        )

    logging.info(

        f"Collected {len(new_records)} new articles."

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
    # Remove empty title
    # --------------------------------------------------------

    merged_df.dropna(

        subset=["title"],

        inplace=True

    )

    # --------------------------------------------------------
    # Convert publish date
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
    # Metadata
    # --------------------------------------------------------

    crawl_time = pd.Timestamp.now()

    merged_df["crawl_time"] = crawl_time

    merged_df["source_system"] = SOURCE_SYSTEM

    # --------------------------------------------------------
    # Column order
    # --------------------------------------------------------

    merged_df = merged_df[

        [

            "article_id",

            "publish_date",

            "title",

            "summary",

            "url",

            "source",

            "source_system",

            "crawl_time"

        ]

    ]

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

def crawl_baodanang():

    logging.info("=" * 60)
    logging.info("BAO DANANG PIPELINE STARTED")
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
    # Update Checkpoint
    # --------------------------------------------------------

    update_checkpoint(

        merged_df,

        new_records

    )

    elapsed = time.time() - start_time

    logging.info("=" * 60)
    logging.info("BAO DANANG PIPELINE FINISHED")
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

    crawl_baodanang()