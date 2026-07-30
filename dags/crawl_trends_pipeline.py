# ============================================================
# IMPORT
# ============================================================

import logging

from crawl_trends import crawl_trends

from upload_gcs import (

    download_files,

    upload_files

)

logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(message)s"

)

BUCKET_FOLDER = "raw/trends"

TREND_FILES = [

    "google_trends_long.csv",

    "google_trends_wide.csv",

    "google_trends_checkpoint.json"

]


# ============================================================
# MAIN PIPELINE
# ============================================================

def crawl_trends_pipeline():

    logging.info("=" * 60)
    logging.info("GOOGLE TRENDS PIPELINE STARTED")
    logging.info("=" * 60)

    # --------------------------------------------------------
    # Download historical files
    # --------------------------------------------------------

    download_files(

        files=TREND_FILES,

        bucket_folder=BUCKET_FOLDER

    )

    # --------------------------------------------------------
    # Crawl
    # --------------------------------------------------------

    crawl_trends()

    # --------------------------------------------------------
    # Upload updated files
    # --------------------------------------------------------

    upload_files(

        files=TREND_FILES,

        bucket_folder=BUCKET_FOLDER

    )

    logging.info("=" * 60)
    logging.info("GOOGLE TRENDS PIPELINE FINISHED")
    logging.info("=" * 60)


# ============================================================
# LOCAL TEST
# ============================================================

if __name__ == "__main__":

    crawl_trends_pipeline()