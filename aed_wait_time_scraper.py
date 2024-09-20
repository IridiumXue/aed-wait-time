import requests
import json
from datetime import datetime, timedelta
import os
from huggingface_hub import HfApi, create_repo

print("Script started")
print(f"HF_TOKEN is set: {'HF_TOKEN' in os.environ}")
print(f"SCRAPE_MODE: {os.environ.get('SCRAPE_MODE', 'Not set')}")

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
    current_date = datetime.now().strftime("%Y-%m-%d")
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
        "timestamp": timestamp.isoformat() if timestamp else datetime.now().isoformat(),
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
    now = datetime.now()
    readme_content = f"""# AED Wait Time Data

Last updated: {now.isoformat()}

This dataset contains AED wait time data for Hong Kong public hospitals. Data is organized in daily files and includes both English and Chinese hospital names.

Note: The data is collected every 15 minutes. If the script runs between these 15-minute intervals, it will retrieve data for the most recent 15-minute period.

Note: Chinese hospital names are stored under the 'hospNameCh' key in our dataset, but they are originally from the 'hospNameGb' field in the source API.

Note: All JSON files in this dataset are UTF-8 encoded and contain Chinese characters. If you see Unicode escape sequences (e.g., \\u4ec1\\u6d4e\\u533b\\u9662) instead of Chinese characters when viewing the raw JSON on the Hugging Face website, this is normal and does not affect the data integrity. You can download the JSON file and open it in a UTF-8 compatible editor to view the Chinese characters directly.
"""
    api.upload_file(
        path_or_fileobj=readme_content.encode('utf-8'),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
    )
    print("README updated successfully")

def log_error(error_message, repo_id):
    print(f"Logging error: {error_message}")
    api = HfApi()
    current_date = datetime.now().strftime("%Y-%m-%d")
    error_log = {
        "timestamp": datetime.now().isoformat(),
        "error": error_message
    }
    error_filename = f"error_{current_date}.json"
    
    # Check if error file for today exists
    try:
        existing_content = api.hf_hub_download(repo_id=repo_id, filename=f"errors/{error_filename}", repo_type="dataset")
        with open(existing_content, 'r', encoding='utf-8') as f:
            existing_errors = json.load(f)
    except:
        existing_errors = {"errors": []}
    
    existing_errors["errors"].append(error_log)
    json_error = json.dumps(existing_errors, ensure_ascii=False, indent=2)
    
    api.upload_file(
        path_or_fileobj=json_error.encode('utf-8'),
        path_in_repo=f"errors/{error_filename}",
        repo_id=repo_id,
        repo_type="dataset"
    )
    print("Error logged successfully")

def check_and_update(repo_id):
    print("Checking and updating data...")
    api = HfApi()
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    
    try:
        # Try to download today's file
        filename = f"data_{current_date}.json"
        file_content = api.hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset")
        with open(file_content, 'r', encoding='utf-8') as f:
            daily_data = json.load(f)
        
        if daily_data["data"]:
            last_entry_time = datetime.fromisoformat(daily_data["data"][-1]["timestamp"])
            
            # Calculate the start of the current 15-minute interval
            current_interval_start = now.replace(minute=now.minute - now.minute % 15, second=0, microsecond=0)
            
            if last_entry_time < current_interval_start:
                print(f"Data missing for the interval starting at {current_interval_start}. Fetching new data...")
                data = fetch_data()
                update_dataset(data, repo_id, timestamp=current_interval_start)
                update_readme(repo_id)
                print("Data updated successfully.")
            else:
                print("Data is up to date. No action needed.")
        else:
            print("Today's file is empty. Fetching new data...")
            data = fetch_data()
            update_dataset(data, repo_id)
            update_readme(repo_id)
            print("Data updated successfully.")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        print("Fetching new data and creating/updating file...")
        data = fetch_data()
        update_dataset(data, repo_id)
        update_readme(repo_id)
        print("Data updated successfully.")

def main():
    print("Main function started")
    repo_id = "StannumX/aed-wait-time-data"
    scrape_mode = os.environ.get('SCRAPE_MODE', 'NORMAL')
    print(f"Scrape mode: {scrape_mode}")
    
    if scrape_mode == 'NORMAL':
        try:
            data = fetch_data()
            now = datetime.now()
            current_interval_start = now.replace(minute=now.minute - now.minute % 15, second=0, microsecond=0)
            update_dataset(data, repo_id, timestamp=current_interval_start)
            update_readme(repo_id)
            print(f"Data updated successfully at {now.isoformat()}")
        except Exception as e:
            error_message = f"Failed to fetch and update data: {str(e)}"
            print(error_message)
            log_error(error_message, repo_id)
    elif scrape_mode == 'CHECK':
        check_and_update(repo_id)
    else:
        print(f"Unknown scrape mode: {scrape_mode}")

if __name__ == "__main__":
    main()
    print("Script completed")
