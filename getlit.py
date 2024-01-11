import json
import boto3
import os
from datetime import datetime, timedelta
from paperscraper.get_dumps import medrxiv, biorxiv, chemrxiv
from dotenv import load_dotenv, find_dotenv; load_dotenv(find_dotenv())

os.environ['AWS_ACCESS_KEY_ID'] = f"{os.getenv('AWS_KEY')}"
os.environ['AWS_SECRET_ACCESS_KEY'] = f"{os.getenv('AWS_SECRET')}"
paperscraper_lib_path = f"{os.getenv('PAPERSCRAPER_LIB_PATH')}"
project_path = f"{os.getenv('PROJECT_PATH')}"
bucket = f"{os.getenv('BUCKET')}"

def query_xrxiv(rxiv):
    # Query for new papers from yesterday to today
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    if rxiv == "medrxiv":
        medrxiv(begin_date=yesterday, end_date=today)
    elif rxiv == "biorxiv":
        biorxiv(begin_date=yesterday, end_date=today)
    elif rxiv == "chemrxiv":
        chemrxiv(begin_date=yesterday, end_date=today)

def append_to_master_jsonl(new_file, master_file):
    with open(new_file, 'r') as nf:
        new_data = [json.loads(line) for line in nf.readlines()]
    
    if os.path.exists(master_file):
        with open(master_file, 'r+') as mf:
            master_data = [json.loads(line) for line in mf.readlines()]
            master_data.extend(new_data)
            mf.seek(0)
            mf.truncate()
            for entry in master_data:
                mf.write(json.dumps(entry) + '\n')
    else:
        with open(master_file, 'w') as mf:
            for entry in new_data:
                mf.write(json.dumps(entry) + '\n')

def upload_to_s3(file_path, bucket, object_name):
 
    s3_client = boto3.resource('s3')
    s3_client.Bucket(bucket).upload_file(file_path, object_name)


def load_jsonl_file(file_path):
    with open(file_path, 'r') as file:
        return [json.loads(line) for line in file]

def record_exists(master_records, new_record, unique_field='doi'):
    return any(new_record.get(unique_field) == master_record.get(unique_field) for master_record in master_records)

def append_new_records(master_file_path, new_file_path):
    master_records = load_jsonl_file(master_file_path)
    new_records = load_jsonl_file(new_file_path)

    # Check if the first and last records of the new file are in the master file
    if record_exists(master_records, new_records[0]) and record_exists(master_records, new_records[-1]):
        print("Both the first and last records of the new file already exist in the master file.")
        return

    # Append new records to master file
    with open(master_file_path, 'a') as master_file:
        for record in new_records:
            json.dump(record, master_file)
            master_file.write('\n')

def main():

    today = datetime.now().strftime('%Y-%m-%d')
    # File paths

    print("Querying medrxiv")
    query_xrxiv("medrxiv")
    new_xrxiv_file = f'{paperscraper_lib_path}/medrxiv_{today}.jsonl'
    master_xrxiv_file_path = f'{project_path}/medrxiv_master.jsonl'

    print(f"Appending new records from {new_xrxiv_file} to {master_xrxiv_file_path}")
    append_new_records(master_xrxiv_file_path, new_xrxiv_file)

    # # Upload to S3
    s3_object_name = 'medrxiv.jsonl'
    print(f"Uploading new master {master_xrxiv_file_path} as {s3_object_name} to {bucket}")
    upload_to_s3(master_xrxiv_file_path, bucket, s3_object_name)

    # Clean up local new file
    print(f"Cleaning up new file {new_xrxiv_file}")
    os.remove(new_xrxiv_file)



    print("Querying biorxiv")
    query_xrxiv("biorxiv")
    new_xrxiv_file = f'{paperscraper_lib_path}/biorxiv_{today}.jsonl'
    master_xrxiv_file_path = f'{project_path}/biorxiv_master.jsonl'

    append_new_records(master_xrxiv_file_path, new_xrxiv_file)

    # # Upload to S3
    s3_object_name = 'biorxiv.jsonl'
    print(f"Uploading new master {master_xrxiv_file_path} as {s3_object_name} to {bucket}")
    upload_to_s3(master_xrxiv_file_path, bucket, s3_object_name)

    # Clean up local new file
    print(f"Cleaning up new file {new_xrxiv_file}")
    os.remove(new_xrxiv_file)



    print("Querying chemrxiv")
    query_xrxiv("chemrxiv")
    new_xrxiv_file = f'{paperscraper_lib_path}/chemrxiv_{today}.jsonl'
    master_xrxiv_file_path = f'{project_path}/chemrxiv_master.jsonl'

    append_new_records(master_xrxiv_file_path, new_xrxiv_file)

    # # Upload to S3
    s3_object_name = 'chemrxiv.jsonl'
    print(f"Uploading new master {master_xrxiv_file_path} as {s3_object_name} to {bucket}")
    upload_to_s3(master_xrxiv_file_path, bucket, s3_object_name)

    # Clean up local new file
    print(f"Cleaning up new file {new_xrxiv_file}")
    os.remove(new_xrxiv_file)


if __name__ == "__main__":
    main()
