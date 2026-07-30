# ============================================================
# IMPORT LIBRARIES
# ============================================================

from pytrends.request import TrendReq
from datetime import date
from datetime import datetime
from datetime import timedelta
from functools import reduce
import pandas as pd
import logging
import json
import os
import time
import random # [BỔ SUNG] Thư viện để lấy ngẫu nhiên User-Agent và thời gian chờ


# ============================================================
# CONFIGURATION
# ============================================================

INITIAL_START_DATE = "2020-01-01"
WINDOW_DAYS = 29
CHECKPOINT_FILE = "google_trends_checkpoint.json"
OUTPUT_WIDE = "google_trends_wide.csv"
OUTPUT_LONG = "google_trends_long.csv"
ANCHOR_KEYWORD = "danang"

KEYWORD_GROUPS = {
    "group_1": ["danang", "danang city", "da nang vietnam", "da nang travel"],
    "group_2": ["danang", "bana hills danang", "dragon bridge da nang", "lady buddha danang", "marble mountain da nang"],
    "group_3": ["danang", "da nang hotel", "danang beach hotel", "da nang resort", "hotel in da nang"],
    "group_4": ["danang", "danang international airport", "motorbike rental da nang", "danang airport transfer"],
    "group_5": ["danang", "son tra night market", "danang beach", "things to do in da nang", "weather da nang"]
}


# ============================================================
# LOGGER
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ============================================================
# PYTRENDS CLIENT (NÂNG CẤP CHỐNG BOT)
# ============================================================

# [BỔ SUNG] Danh sách các trình duyệt giả mạo
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/114.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
]

def get_trend_req():
    """Khởi tạo kết nối với Google Trends giả mạo trình duyệt thật"""
    user_agent = random.choice(USER_AGENTS)
    return TrendReq(
        hl="en-US",
        tz=360,
        timeout=(15, 30), # Kéo dài thời gian đợi máy chủ phản hồi
        requests_args={'headers': {'User-Agent': user_agent}}
    )


# ============================================================
# LOAD CHECKPOINT
# ============================================================

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        logging.info("No checkpoint found.")
        return None
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        checkpoint = json.load(f)
    logging.info(f"Checkpoint loaded: {checkpoint['last_date']}")
    return checkpoint


# ============================================================
# SAVE CHECKPOINT
# ============================================================

def save_checkpoint(latest_date):
    checkpoint = {"last_date": latest_date}
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=4, ensure_ascii=False)
    logging.info(f"Checkpoint updated: {latest_date}")


# ============================================================
# LOAD OLD DATA
# ============================================================

def load_old_data():
    if not os.path.exists(OUTPUT_LONG):
        logging.info("No previous Google Trends data.")
        return pd.DataFrame()
    df = pd.read_csv(OUTPUT_LONG, parse_dates=["search_date"])
    df.sort_values("search_date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    logging.info(f"Loaded {len(df)} historical records.")
    return df

# ============================================================
# DETERMINE CRAWL WINDOW
# ============================================================

def get_crawl_window():
    checkpoint = load_checkpoint()
    end_date = date.today()

    if checkpoint is None:
        start_date = datetime.strptime(INITIAL_START_DATE, "%Y-%m-%d").date()
        logging.info(f"Initial Crawl : {start_date} -> {end_date}")
        return start_date, end_date

    last_date = datetime.strptime(checkpoint["last_date"], "%Y-%m-%d").date()
    start_date = last_date - timedelta(days=WINDOW_DAYS)
    logging.info(f"Incremental Crawl : {start_date} -> {end_date}")
    return start_date, end_date


# ============================================================
# CRAWL ONE KEYWORD GROUP (NÂNG CẤP CHỐNG 429)
# ============================================================

def crawl_single_group(keywords, start_date, end_date, retries=3):
    timeframe = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
    logging.info(f"Timeframe : {timeframe}")

    for attempt in range(retries):
        try:
            # [BỔ SUNG] 1. Gọi hàm kết nối mới với User-Agent ngẫu nhiên
            pytrends = get_trend_req()
            
            # [BỔ SUNG] 2. Ngủ ngẫu nhiên 5-15s để bắt chước con người
            sleep_time = random.uniform(5.0, 15.0)
            logging.info(f"Đang chờ {sleep_time:.1f}s trước khi gửi yêu cầu...")
            time.sleep(sleep_time)

            pytrends.build_payload(kw_list=keywords, timeframe=timeframe, geo="")
            df = pytrends.interest_over_time()

            if df.empty:
                raise ValueError("Google Trends returned empty dataframe.")

            df = df.drop(columns=["isPartial"], errors="ignore")
            df.reset_index(inplace=True)
            logging.info(f"SUCCESS : {keywords}")
            return df

        except Exception as e:
            error_msg = str(e)
            
            # [BỔ SUNG] 3. Xử lý riêng biệt cho lỗi 429 (Bị chặn)
            if '429' in error_msg:
                wait_time = 120 * (attempt + 1) # Nghỉ 2 phút, 4 phút, 6 phút
                logging.warning(f"Lỗi 429: Bị Google chặn. Chờ {wait_time}s để phục hồi... (Lần {attempt+1}/{retries})")
            else:
                wait_time = 30 * (attempt + 1)
                logging.warning(f"Lỗi khác: {e}. Chờ {wait_time}s... (Lần {attempt+1}/{retries})")
            
            time.sleep(wait_time)

    raise RuntimeError(f"FAILED : {keywords}")

# ============================================================
# MERGE DATA
# ============================================================

def merge_data(old_df, new_df, start_date):
    if old_df.empty:
        logging.info("Initial dataset created.")
        return new_df

    old_df["search_date"] = pd.to_datetime(old_df["search_date"])
    cutoff_date = pd.to_datetime(start_date)
    old_df = old_df[old_df["search_date"] < cutoff_date]

    merged_df = pd.concat([old_df, new_df], ignore_index=True)
    merged_df.drop_duplicates(subset=["search_date", "keyword"], keep="last", inplace=True)
    merged_df.sort_values(by=["search_date", "keyword"], inplace=True)
    merged_df.reset_index(drop=True, inplace=True)
    merged_df["search_interest"] = merged_df["search_interest"].astype(int)
    logging.info(f"Merged records : {len(merged_df)}")
    return merged_df


# ============================================================
# SAVE DATA
# ============================================================

def save_data(google_trends_long):
    google_trends_long.to_csv(OUTPUT_LONG, index=False, encoding="utf-8-sig")
    logging.info(f"Saved : {OUTPUT_LONG}")

    google_trends_wide = (
        google_trends_long
        .pivot_table(index="search_date", columns="keyword", values="search_interest", aggfunc="last")
        .reset_index()
    )
    google_trends_wide.reset_index(drop=True, inplace=True)
    google_trends_wide.columns.name = None
    google_trends_wide.sort_values(by="search_date", inplace=True)
    google_trends_wide.to_csv(OUTPUT_WIDE, index=False, encoding="utf-8-sig")
    logging.info(f"Saved : {OUTPUT_WIDE}")


# ============================================================
# MAIN PIPELINE
# ============================================================

def crawl_trends():
    logging.info("=" * 60)
    logging.info("GOOGLE TRENDS PIPELINE STARTED")
    logging.info("=" * 60)
    start_time = time.time()

    old_df = load_old_data()
    start_date, end_date = get_crawl_window()
    all_dfs = []

    for group_name, keywords in KEYWORD_GROUPS.items():
        logging.info(f"Crawling {group_name}")
        df = crawl_single_group(keywords=keywords, start_date=start_date, end_date=end_date)
        all_dfs.append(df)

    google_trends_wide = all_dfs[0]
    for df in all_dfs[1:]:
        google_trends_wide = google_trends_wide.merge(
            df.drop(columns=[ANCHOR_KEYWORD]),
            on="date",
            how="outer"
        )

    google_trends_wide = google_trends_wide.loc[:, ~google_trends_wide.columns.duplicated()]
    google_trends_wide.sort_values(by="date", inplace=True)
    google_trends_wide.reset_index(drop=True, inplace=True)

    google_trends_long = google_trends_wide.melt(id_vars="date", var_name="keyword", value_name="search_interest")
    google_trends_long.dropna(subset=["search_interest"], inplace=True)
    google_trends_long.rename(columns={"date":"search_date"}, inplace=True)
    
    google_trends_long["search_date"] = pd.to_datetime(google_trends_long["search_date"])
    google_trends_long["search_interest"] = google_trends_long["search_interest"].astype(int)
    google_trends_long["source"] = "google_trends"
    google_trends_long["source_system"] = "pytrends"
    google_trends_long["crawl_time"] = pd.Timestamp.now()

    merged_df = merge_data(old_df=old_df, new_df=google_trends_long, start_date=start_date)
    save_data(merged_df)

    latest_date = merged_df["search_date"].max().strftime("%Y-%m-%d")
    save_checkpoint(latest_date)

    elapsed = time.time() - start_time
    logging.info("=" * 60)
    logging.info("GOOGLE TRENDS PIPELINE FINISHED")
    logging.info("=" * 60)
    logging.info(f"Total Records : {len(merged_df)}")
    logging.info(f"Latest Date : {latest_date}")
    logging.info(f"Elapsed : {elapsed/60:.2f} minutes")

    return {"wide_file": OUTPUT_WIDE, "long_file": OUTPUT_LONG}


# ============================================================
# RUN LOCALLY
# ============================================================

if __name__ == "__main__":
    crawl_trends()