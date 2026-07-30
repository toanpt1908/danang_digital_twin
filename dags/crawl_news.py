# ============================================================
# IMPORT LIBRARIES
# ============================================================

import logging

from crawl_vnexpress import crawl_vnexpress
from crawl_thanhnien import crawl_thanhnien
from crawl_baodanang import crawl_baodanang

from upload_gcs import (

    download_files,

    upload_files

)


# ============================================================
# LOGGER
# ============================================================

logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(message)s"

)


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET_FOLDER = "raw/news"

NEWS_FILES = [

    "vnexpress_news.csv",
    "vnexpress_checkpoint.json",

    "thanhnien_news.csv",
    "thanhnien_checkpoint.json",

    "baodanang_news.csv",
    "baodanang_checkpoint.json"

]


# ============================================================
# MAIN NEWS PIPELINE
# ============================================================

def crawl_news():

    logging.info("=" * 60)
    logging.info("NEWS PIPELINE STARTED")
    logging.info("=" * 60)

    # --------------------------------------------------------
    # Download latest CSV + checkpoint
    # --------------------------------------------------------

    download_files(

        files=NEWS_FILES,

        bucket_folder=BUCKET_FOLDER

    )

    # --------------------------------------------------------
    # Crawl VnExpress
    # --------------------------------------------------------

    logging.info("===== Crawl VnExpress =====")

    crawl_vnexpress()

    # --------------------------------------------------------
    # Crawl Thanh Nien
    # --------------------------------------------------------

    logging.info("===== Crawl Thanh Nien =====")

    crawl_thanhnien()

    # --------------------------------------------------------
    # Crawl Bao Da Nang
    # --------------------------------------------------------

    logging.info("===== Crawl Bao Da Nang =====")

    crawl_baodanang()

    # --------------------------------------------------------
    # Upload updated CSV + checkpoint
    # --------------------------------------------------------

    upload_files(

        files=NEWS_FILES,

        bucket_folder=BUCKET_FOLDER

    )

    logging.info("=" * 60)
    logging.info("NEWS PIPELINE FINISHED")
    logging.info("=" * 60)

    return True


# ============================================================
# RUN LOCALLY
# ============================================================

if __name__ == "__main__":

    crawl_news()