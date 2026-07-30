import time
import os
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import storage
import undetected_chromedriver as uc

# Khai báo khóa Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\admin\Downloads\gcp_service_account_key.json"

PROJECT_ID = "project-7cfdad94-4b3b-452c-8da"
BUCKET_NAME = "datalake_cap2"

VN_AIRPORTS_IATA = {
    "SGN", "HAN", "DAD", "CXR", "PQC", "HPH", "VII", "VDH", "UIH", 
    "VCA", "VKG", "VCS", "DLI", "TBB", "HUI", "PXU", "BMV", "VDO", 
    "THD", "DIN", "VCL"
}

def parse_fr24_date(date_str, target_year):
    try:
        month_day = date_str.split(", ")[1]
        return datetime.strptime(month_day, "%b %d").replace(year=target_year).date()
    except Exception:
        return None

def scrape_and_upload():
    # CHÚ Ý: Đổi thành ngày hôm nay vì script sẽ chạy lúc 12:00 trưa
    target_date = datetime.now().date()
    target_date_str = target_date.strftime("%Y-%m-%d")
    print(f"Bắt đầu cào dữ liệu cho ngày hôm nay: {target_date_str}")
    
    options = uc.ChromeOptions()
    # options.add_argument("--headless") 
    
    print("Đang khởi động trình duyệt chống phát hiện (Anti-bot)...")
    driver = uc.Chrome(options=options, version_main=150) # Giữ nguyên version_main nếu bạn dùng cách 2
    
    domestic, international = 0, 0
    
    try:
        target_url = "https://www.flightradar24.com/airport/dad/arrivals"
        driver.get(target_url)
        print("Đang chờ trang web tải dữ liệu và vượt Cloudflare...")
        time.sleep(5)
        
        while True:
            # 1. Lướt xuống dưới cùng trang
            print("Đang lướt xuống cuối trang...")
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2) # Chờ DOM tải thêm nếu có
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    print("Đã đến cuối trang hoặc gặp thông báo chặn.")
                    break
                last_height = new_height
            
            # 2. Lướt ngược lên trên cùng trang
            print("Đang lướt lên trên cùng trang...")
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Kiểm tra xem đã lấy đủ dữ liệu chưa
            soup = BeautifulSoup(driver.page_source, "html.parser")
            h3_tags = soup.find_all("h3", class_=lambda c: c and "inline-flex items-center text-sm" in c)
            
            oldest_date_on_page = None
            for tag in reversed(h3_tags):
                parsed = parse_fr24_date(tag.text.strip(), target_date.year)
                if parsed:
                    oldest_date_on_page = parsed
                    break
            
            # Dừng nếu ngày cũ nhất trên màn hình lùi về trước ngày hôm nay (tức là đã sang hôm qua)
            if oldest_date_on_page and oldest_date_on_page < target_date:
                print(f"Đã quét đủ dữ liệu từ đầu ngày {target_date_str}.")
                break
                
            # 3. Nhấn "Earlier flights"
            try:
                earlier_btn_xpath = "//button[@data-testid='airport-panel__schedules__earlier-flights']"
                earlier_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, earlier_btn_xpath))
                )
                print("Đang click nút 'Earlier flights'...")
                earlier_button.click()
                time.sleep(4)
            except:
                print("Không tìm thấy nút 'Earlier flights' hoặc đã hết lịch sử.")
                break

        print("Đang bóc tách dữ liệu chuyến bay...")
        final_soup = BeautifulSoup(driver.page_source, "html.parser")
        elements = final_soup.find_all(["h3", "span"])
        current_block_date = None
        
        for el in elements:
            if el.name == "h3" and "inline-flex items-center text-sm" in " ".join(el.get("class", [])):
                current_block_date = parse_fr24_date(el.text.strip(), target_date.year)
            elif el.name == "span" and "border-gray-700" in " ".join(el.get("class", [])) and "text-gray-1000" in " ".join(el.get("class", [])):
                if current_block_date == target_date:
                    origin_code = el.text.strip()
                    if origin_code in VN_AIRPORTS_IATA:
                        domestic += 1
                    else:
                        international += 1

        total = domestic + international
        
        if total > 0:
            row = {
                "flight_date": target_date_str,
                "domestic_flights": domestic,
                "international_flights": international,
                "total_flights": total,
                "data_source": "Flightradar24"
            }
            local_csv_path = f"{target_date_str}_summary.csv"
            pd.DataFrame([row]).to_csv(local_csv_path, index=False)
            print(f"Đã lưu CSV tạm tại máy: {local_csv_path}")
            
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            gcs_blob_name = f"bronze/flight/{target_date_str}/summary.csv"
            blob = bucket.blob(gcs_blob_name)
            
            blob.upload_from_filename(local_csv_path)
            print(f"Đã tải thành công lên GCS: gs://{BUCKET_NAME}/{gcs_blob_name}")
            
            os.remove(local_csv_path)
        else:
            print(f"Không có dữ liệu chuyến bay cho ngày {target_date_str}.")

    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_and_upload()