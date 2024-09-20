import requests
import json
from datetime import datetime, timedelta
import os
import pytz
from huggingface_hub import HfApi, create_repo

print("Script started")
print(f"HF_TOKEN is set: {'HF_TOKEN' in os.environ}")

# 设置香港时区
hk_tz = pytz.timezone('Asia/Hong_Kong')

def get_hk_time():
    return datetime.now(hk_tz)

def get_next_check_time(current_time):
    """计算下一个检查时间点"""
    hour = current_time.hour
    minute = current_time.minute
    
    if minute < 1:
        next_minute = 1
    elif minute < 16:
        next_minute = 16
    elif minute < 31:
        next_minute = 31
    elif minute < 46:
        next_minute = 46
    else:
        next_minute = 1
        hour = (hour + 1) % 24
    
    next_time = current_time.replace(hour=hour, minute=next_minute, second=0, microsecond=0)
    if next_time <= current_time:
        next_time += timedelta(hours=1)
    
    return next_time

def fetch_data():
    print("Fetching data...")
    url = "https://www.ha.org.hk/aedwt/data/aedWtData.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        extracted_data = []
        for hospital in data['result']['hospData']:
            hospital_data = {
                "hospNameEn": hospital.get('hospNameEn', ''),
                "hospNameCh": hospital.get('hospNameGb', ''),  # 使用 'hospNameGb' 作为中文名
                "topWait": hospital.get('topWait', ''),
                "hospTimeEn": hospital.get('hospTimeEn', '')
            }
            extracted_data.append(hospital_data)
        print(f"Data fetched successfully. Number of hospitals: {len(extracted_data)}")
        return extracted_data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def update_dataset(new_data, repo_id, timestamp=None):
    print(f"Updating dataset: {repo_id}")
    api = HfApi()
    
    # Ensure the repo exists
    try:
        create_repo(repo_id, repo_type="dataset", exist_ok=True)
    except Exception as e:
        print(f"Error creating repo: {str(e)}")
    
    # Get current date for file naming
    current_date = get_hk_time().strftime("%Y-%m-%d")
    filename = f"data_{current_date}.json"
    
    # Check if the file for today already exists
    try:
        existing_content = api.hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset")
        with open(existing_content, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    except Exception as e:
        print(f"Error loading existing data: {str(e)}. Creating new file.")
        existing_data = {"data": []}
    
    # Append new data to existing data
    existing_data["data"].append({
        "timestamp": timestamp.isoformat() if timestamp else get_hk_time().isoformat(),
        "hospitals": new_data
    })
    
    # Convert data to JSON string, ensure_ascii=False to properly encode Chinese characters
    json_data = json.dumps(existing_data, ensure_ascii=False, indent=2)
    
    # Upload the file
    api.upload_file(
        path_or_fileobj=json_data.encode('utf-8'),
        path_in_repo=filename,
        repo_id=repo_id,
        repo_type="dataset"
    )
    print(f"Data file {filename} updated successfully")

def update_readme(repo_id):
    print("Updating README...")
    api = HfApi()
    now = get_hk_time()
    readme_content = f"""# AED Wait Time Data

Last updated: {now.isoformat()} (Hong Kong Time)

This dataset contains AED wait time data for Hong Kong public hospitals. Data is organized in daily files and includes both English and Chinese hospital names.

Note: The data is collected at fixed time points: 01, 16, 31, and 46 minutes past each hour.

Note: Chinese hospital names are stored under the 'hospNameCh' key in our dataset, but they are originally from the 'hospNameGb' field in the source API.

Note: All timestamps in this dataset are in Hong Kong Time (HKT, UTC+8).

Note: All JSON files in this dataset are UTF-8 encoded and contain Chinese characters. If you see Unicode escape sequences (e.g., \\u4ec1\\u6d4e\\u533b\\u9662) instead of Chinese characters when viewing the raw JSON on the Hugging Face website, this is normal and does not affect the data integrity. You can download the JSON file and open it in a UTF-8 compatible editor to view the Chinese characters directly.
"""
    api.upload_file(
        path_or_fileobj=readme_content.encode('utf-8'),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
    )
    print("README updated successfully")

def should_update(last_update_time):
    now = get_hk_time()
    next_check_time = get_next_check_time(last_update_time)
    return now >= next_check_time

def check_and_update(repo_id):
    print("Checking and updating data...")
    api = HfApi()
    now = get_hk_time()
    current_date = now.strftime("%Y-%m-%d")
    
    try:
        # Try to download today's file
        filename = f"data_{current_date}.json"
        file_content = api.hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset")
        with open(file_content, 'r', encoding='utf-8') as f:
            daily_data = json.load(f)
        
        if daily_data["data"]:
            last_entry_time = datetime.fromisoformat(daily_data["data"][-1]["timestamp"]).replace(tzinfo=hk_tz)
            print(f"Last update time: {last_entry_time.isoformat()}")
            
            if should_update(last_entry_time):
                print(f"Updating data for {now.strftime('%Y-%m-%d %H:%M')}...")
                data = fetch_data()
                update_dataset(data, repo_id, timestamp=now)
                update_readme(repo_id)
                print("Data updated successfully.")
            else:
                next_check = get_next_check_time(now)
                print(f"Not yet time for the next update. Next check at: {next_check.strftime('%Y-%m-%d %H:%M')}")
        else:
            print("Today's file is empty. Fetching new data...")
            data = fetch_data()
            update_dataset(data, repo_id, timestamp=now)
            update_readme(repo_id)
            print("Data updated successfully.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        print("Fetching new data and creating/updating file...")
        data = fetch_data()
        update_dataset(data, repo_id, timestamp=now)
        update_readme(repo_id)
        print("Data updated successfully.")

def main():
    print("Main function started")
    repo_id = "StannumX/aed-wait-time-data"
    now = get_hk_time()
    print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    check_and_update(repo_id)
    print("Script completed")

if __name__ == "__main__":
    main()
