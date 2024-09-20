import requests
import json
from datetime import datetime, timedelta
from datasets import load_dataset, Dataset
from huggingface_hub import HfApi
import os

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
            extracted_data.append({
                "hospNameGb": hospital['hospNameGb'],
                "topWait": hospital['topWait'],
                "hospTimeEn": hospital['hospTimeEn']
            })
        print(f"Data fetched successfully. Number of hospitals: {len(extracted_data)}")
        return extracted_data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

def update_dataset(new_data, dataset_path):
    print(f"Updating dataset: {dataset_path}")
    try:
        dataset = load_dataset(dataset_path, split="train")
        updated_data = dataset.add_item({"data": new_data})
    except Exception as e:
        print(f"Error loading dataset: {str(e)}. Creating new dataset.")
        updated_data = Dataset.from_dict({"data": [new_data]})

    updated_data.push_to_hub(dataset_path)
    print("Dataset updated successfully")

def update_readme(now):
    print("Updating README...")
    api = HfApi()
    readme_content = f"# AED Wait Time Data\n\nLast updated: {now.isoformat()}\n\nThis dataset contains AED wait time data for Hong Kong public hospitals."
    api.upload_file(
        path_or_fileobj=readme_content.encode(),
        path_in_repo="README.md",
        repo_id="StannumX/aed-wait-time-data",
        repo_type="dataset",
    )
    print("README updated successfully")

def log_error(error_message):
    print(f"Logging error: {error_message}")
    error_log = {
        "timestamp": datetime.now().isoformat(),
        "error": error_message
    }
    error_path = "StannumX/aed-wait-time-data/errors"
    try:
        dataset = load_dataset(error_path, split="train")
        updated_data = dataset.add_item(error_log)
    except Exception as e:
        print(f"Error loading error dataset: {str(e)}. Creating new error dataset.")
        updated_data = Dataset.from_dict({"errors": [error_log]})
    
    updated_data.push_to_hub(error_path)
    print("Error logged successfully")

def check_and_update():
    print("Checking and updating data...")
    now = datetime.now()
    fifteen_min_ago = now - timedelta(minutes=15)
    year_month = fifteen_min_ago.strftime("%Y-%m")
    day = fifteen_min_ago.strftime("%d")
    
    data_path = f"StannumX/aed-wait-time-data/{year_month}/{day}"
    
    try:
        dataset = load_dataset(data_path, split="train")
        last_entry = dataset[-1]['data']
        last_entry_time = datetime.strptime(last_entry[0]['hospTimeEn'], "%d/%m/%Y %I:%M%p")
        
        if last_entry_time < fifteen_min_ago:
            print("Data missing for the last 15 minutes. Fetching new data...")
            data = fetch_data()
            update_dataset(data, data_path)
            update_readme(now)
            print("Data updated successfully.")
        else:
            print("Data is up to date. No action needed.")
    except Exception as e:
        error_message = f"Error checking or updating data: {str(e)}"
        print(error_message)
        log_error(error_message)

def main():
    print("Main function started")
    scrape_mode = os.environ.get('SCRAPE_MODE', 'NORMAL')
    print(f"Scrape mode: {scrape_mode}")
    
    if scrape_mode == 'NORMAL':
        try:
            data = fetch_data()
            now = datetime.now()
            year_month = now.strftime("%Y-%m")
            day = now.strftime("%d")
            data_path = f"StannumX/aed-wait-time-data/{year_month}/{day}"
            update_dataset(data, data_path)
            update_readme(now)
            print(f"Data updated successfully at {now.isoformat()}")
        except Exception as e:
            error_message = f"Failed to fetch and update data: {str(e)}"
            print(error_message)
            log_error(error_message)
    elif scrape_mode == 'CHECK':
        check_and_update()
    else:
        print(f"Unknown scrape mode: {scrape_mode}")

if __name__ == "__main__":
    main()
    print("Script completed")
