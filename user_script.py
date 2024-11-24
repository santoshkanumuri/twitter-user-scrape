# your_script.py

import pandas as pd
import requests
import time
import os
import boto3
from datetime import datetime
import xlsxwriter
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Get Twitter API bearer token from environment variable
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")

# AWS S3 bucket name and region
S3_BUCKET_NAME = "twitter-user"
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")  # Default region

# Function to fetch user data
def fetch_user_data(user_id):
    url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics"
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    try:
        response = requests.get(url, headers=headers)
    except requests.RequestException as e:
        logging.error(f"Request failed for user ID {user_id}: {e}")
        return None, False  # Return data as None, success as False

    if response.status_code == 200:
        data = response.json()
        public_metrics = data.get("data", {}).get("public_metrics", {})
        return {
            "Author ID": user_id,
            "followers_count": public_metrics.get("followers_count"),
            "following_count": public_metrics.get("following_count"),
            "tweet_count": public_metrics.get("tweet_count"),
            "listed_count": public_metrics.get("listed_count"),
            "data_exist": True
        }, True  # Success flag
    elif response.status_code == 429:
        logging.warning("Rate limit exceeded. Stopping execution.")
        return None, "rate_limited"
    elif response.status_code == 404:
        logging.info(f"User ID {user_id} not found.")
        return {
            "Author ID": user_id,
            "followers_count": None,
            "following_count": None,
            "tweet_count": None,
            "listed_count": None,
            "data_exist": False
        }, True  # Data exists is False, but success is True
    elif response.status_code == 403:
        logging.info(f"User ID {user_id} forbidden.")
        return {
            "Author ID": user_id,
            "followers_count": None,
            "following_count": None,
            "tweet_count": None,
            "listed_count": None,
            "data_exist": False
        }, True
    else:
        logging.error(f"Unexpected status code {response.status_code} for user ID {user_id}")
        return None, False

def upload_to_s3(file_name, bucket_name, object_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION
    )
    try:
        extra_args = {}
        if file_name.endswith('.html'):
            extra_args['ContentType'] = 'text/html'
        elif file_name.endswith('.xlsx'):
            extra_args['ContentType'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        # Add any other file types as needed

        s3.upload_file(file_name, bucket_name, object_name, ExtraArgs=extra_args)
        logging.info(f"Uploaded {file_name} to s3://{bucket_name}/{object_name} with ContentType {extra_args.get('ContentType', 'default')}")
        return True
    except Exception as e:
        logging.error(f"Failed to upload {file_name} to S3: {e}")
        return False

def download_from_s3(bucket_name, object_name, file_name):
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION
    )
    try:
        s3.download_file(bucket_name, object_name, file_name)
        logging.info(f"Downloaded {object_name} from s3://{bucket_name} to {file_name}")
        return True
    except s3.exceptions.NoSuchKey:
        logging.warning(f"The object {object_name} does not exist in bucket {bucket_name}. Proceeding without downloading.")
        return False
    except Exception as e:
        logging.error(f"Failed to download {object_name} from S3: {e}")
        return False

def save_to_excel(df, file_name):
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')

    # Get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # Set the format for the 'Author ID' column to text
    text_format = workbook.add_format({'num_format': '@'})
    worksheet.set_column('A:A', 25, text_format)  # Adjust the width as needed

    writer.close()  # Use close() instead of save()
    logging.info(f"Data saved to {file_name}")

# Main script execution
def main():
    # Download id.xlsx from S3
    input_file = "id.xlsx"
    download_success = download_from_s3(S3_BUCKET_NAME, input_file, input_file)
    if not download_success:
        logging.error("Failed to download the input file. Exiting.")
        return

    # Download output.xlsx from S3 (overwrite local file if exists)
    output_file = "output.xlsx"
    download_from_s3(S3_BUCKET_NAME, output_file, output_file)

    # Read input Excel file with 'Author ID' as string
    try:
        df = pd.read_excel(input_file, dtype={'Author ID': str})
        logging.info(f"Loaded {len(df)} user IDs from {input_file}")
    except Exception as e:
        logging.error(f"Failed to read {input_file}: {e}")
        return

    total_ids = set(df["Author ID"].astype(str))  # Set of all IDs from id.xlsx

    # Load output_df from output.xlsx if it exists, otherwise create new DataFrame
    if os.path.exists(output_file):
        try:
            output_df = pd.read_excel(output_file, dtype={'Author ID': str})
            processed_ids = set(output_df["Author ID"])
            logging.info(f"Loaded {len(output_df)} existing records from {output_file}")
        except Exception as e:
            logging.error(f"Failed to read {output_file}: {e}")
            output_df = pd.DataFrame(columns=[
                "Author ID", "followers_count", "following_count", "tweet_count", "listed_count", "data_exist"
            ])
            output_df = output_df.astype({'Author ID': str})
            processed_ids = set()
            logging.info(f"Initialized new DataFrame as {output_file} could not be read.")
    else:
        output_df = pd.DataFrame(columns=[
            "Author ID", "followers_count", "following_count", "tweet_count", "listed_count", "data_exist"
        ])
        output_df = output_df.astype({'Author ID': str})
        processed_ids = set()
        logging.info(f"Initialized new DataFrame as {output_file} does not exist.")

    # Cache for already fetched user data
    fetched_data_cache = {}
    new_data = []

    # Rate limit counter and save interval
    request_count = 0
    save_interval = 100  # Adjust as needed

    # Variable to count new users added today
    new_users_added_today = 0

    try:
        for index, row in df.iterrows():
            user_id = str(row["Author ID"])

            # Skip if already processed
            if user_id in processed_ids:
                continue

            # Check if the user ID is in the cache
            if user_id in fetched_data_cache:
                data = fetched_data_cache[user_id]
                success = True
            else:
                data, success = fetch_user_data(user_id)

                if success == "rate_limited":
                    # Save data collected so far
                    logging.warning(f"Rate limit reached after {request_count} requests. Saving progress and exiting.")
                    if new_data:
                        new_data_df = pd.DataFrame(new_data)
                        output_df = pd.concat([output_df, new_data_df], ignore_index=True)
                        new_data.clear()
                        # Save to Excel using the custom function
                        save_to_excel(output_df, output_file)
                    break  # Exit the loop and script

                if success:
                    fetched_data_cache[user_id] = data  # Cache the data

            if success:
                data['Author ID'] = str(data['Author ID'])  # Ensure 'Author ID' is a string
                new_data.append(data)
                processed_ids.add(user_id)
                new_users_added_today += 1  # Increment the counter
            else:
                logging.warning(f"Failed to fetch data for user ID {user_id}. Skipping.")

            # Increment request_count
            request_count += 1

            # Save data after every save_interval requests
            if request_count % save_interval == 0 and new_data:
                logging.info(f"Saving progress after {request_count} requests...")
                new_data_df = pd.DataFrame(new_data)
                output_df = pd.concat([output_df, new_data_df], ignore_index=True)
                new_data.clear()
                # Save to Excel using the custom function
                save_to_excel(output_df, output_file)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        # Save any remaining data
        if new_data:
            logging.info("Saving remaining new data...")
            new_data_df = pd.DataFrame(new_data)
            output_df = pd.concat([output_df, new_data_df], ignore_index=True)
            new_data.clear()
            # Save to Excel using the custom function
            save_to_excel(output_df, output_file)

        # Ensure 'Author ID's are strings
        output_df['Author ID'] = output_df['Author ID'].astype(str)

        logging.info("Data saved locally.")

        # Calculate users remaining
        users_remaining = len(total_ids - set(output_df["Author ID"]))
        logging.info(f"Users remaining to process: {users_remaining}")

        # Upload output.xlsx to S3
        upload_success = upload_to_s3(output_file, S3_BUCKET_NAME, output_file)

        # Generate and upload index.html to S3
        if upload_success:
            last_updated = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Twitter Data Export</title>
            </head>
            <body>
                <h1>Twitter Data Export</h1>
                <p>Last Updated: {last_updated}</p>
                <p>New Users Added Today: {new_users_added_today}</p>
                <p>Users Remaining: {users_remaining}</p>
                <a href="https://{S3_BUCKET_NAME}.s3.amazonaws.com/{output_file}">Download Data</a>
            </body>
            </html>
            """
            with open('index.html', 'w') as f:
                f.write(html_content)
            upload_success_html = upload_to_s3('index.html', S3_BUCKET_NAME, 'index.html')
            if upload_success_html:
                logging.info("index.html uploaded successfully.")
            else:
                logging.error("Failed to upload index.html to S3.")

        logging.info("Data uploaded to S3. Exiting.")

if __name__ == "__main__":
    main()
