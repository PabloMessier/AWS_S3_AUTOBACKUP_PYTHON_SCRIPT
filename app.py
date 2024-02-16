import boto3
import os
import logging
import botocore
import subprocess
import time
import sys
import datetime
import threading
import json
import curses
from colorama import Fore, init
from s3_filter import s3_compatible_name

init(autoreset=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

logger.addHandler(ch)

def source_directories():
    try:
        with open("file_paths.json", "r") as f:
            data = json.load(f)
            return data["source_directories"]
    except (FileNotFoundError, KeyError):
        logger.warning("NOTICE - Invalid or missing file_paths.json.")
        pause()
        sys.exit()
    except PermissionError:
        logger.error("Permission error accessing file_paths.json.")
        pause()
        sys.exit() 
    except json.decoder.JSONDecodeError:
        logger.warning("Unable to decode file_paths.json.")
        pause()
        sys.exit()

def access():
    try:
        with open("credentials.json", "r") as f:
            credentials = json.load(f)
            access_key = credentials["access_key"]
            secret_access_key = credentials["secret_access_key"]
            BUCKET_NAME = credentials["BUCKET_NAME"]
            return credentials["access_key"], credentials["secret_access_key"], credentials["BUCKET_NAME"]
    except (FileNotFoundError, KeyError):
        logger.warning("Invalid or missing credentials.json.")
        pause()
        sys.exit()
    except ValueError:
        logger.warning("No bucket or credentials found in credentials.json.")
        pause()
        sys.exit()
    except PermissionError:
        logger.error("Permission error accessing credentials.json.")
        pause()
        sys.exit()
    except json.decoder.JSONDecodeError:
        logger.warning("Unable to decode credentials.json file.")
        pause_command()
        sys.exit()

access_key, secret_access_key, BUCKET_NAME = access()
s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

def check_os():
    # Check the operating system type before initializing.
    if os.name == "nt":
        return "Windows"
    elif sys.platform == "mac":
        return "mac"
    else:
        return "Linux"
    
def pause():
    os_type = check_os()
    if os_type == "Windows":
        os.system("pause")
    elif os_type == "mac":
        os.system("read -p 'Press enter to continue...'")
    else:
        # Linux
        os.system("read -p 'Press enter to continue...'")
        
def clear():
    os_type = check_os()
    if os_type == "Windows":
        os.system("cls")
    elif os_type == "mac":
        os.system("clear")
    else:
        # Linux
        os.system("clear")
        
class ColorFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.WARNING:
            return Fore.YELLOW + super().format(record)
        elif record.levelno >= logging.ERROR:
            return Fore.RED + super().format(record)
        else:
            return Fore.WHITE + super().format(record)

# Set up logging handlers: console and file
formatter = ColorFormatter('%(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
file_handler = logging.FileHandler("backup_log.txt", mode="a")
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(console_handler)
logger.addHandler(file_handler)

MAX_RETRIES = 6
INITIAL_WAIT_TIME = 120  # in seconds

def with_retry(func, *args, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except (botocore.exceptions.EndpointConnectionError, 
                botocore.exceptions.ConnectionClosedError, 
                botocore.exceptions.PartialCredentialsError, 
                botocore.exceptions.NoCredentialsError) as e:
            logger.warning(f"Connection issue: {str(e)}. Attempt {attempt+1}/{MAX_RETRIES}. Waiting {INITIAL_WAIT_TIME * (attempt + 1)} seconds before retrying...")
            time.sleep(INITIAL_WAIT_TIME * (attempt + 1))
        except PermissionError:
            logger.error(f"Permission denied. Please check your permissions and try again.")
        except FileNotFoundError:
            logger.error(f"File or directory not found during the backup process.")
    logger.error("Unable to connect, exceeded max attempts.")
    logger.error("Will retry in the next execution cycle.")
    time.sleep(3)
    countdown_termination()

def check_bucket_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError:
        logger.error(f"ERROR -   Bucket {bucket_name} does not exist.")
        pause()
        sys.exit()
        return False

def sync_to_s3(src_directory, s3_bucket):
    # Ensure that source directory path name is compatible with s3
    safe_src_directory = s3_compatible_name(src_directory)
    
    if not os.path.exists(src_directory):
        logger.warning(f"File path {src_directory} does not exist, skipping…")
        time.sleep(2)
        return True

    if not check_bucket_exists(s3_bucket):
        logger.error(f"Bucket {s3_bucket} does not exist.")
        os.sytem("pause")
        sys.exit()
        return False

    s3_uri = f"s3://{s3_bucket}/{os.path.basename(src_directory)}"
    sys.stdout.write(f"\rProcessing {src_directory}...")  
    sys.stdout.flush()
    
    existing_files_in_s3 = set()
    for obj in s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=os.path.basename(src_directory)).get("Contents", []):
        existing_files_in_s3.add(obj["Key"])

    try:
        # Here, we continue to use the AWS CLI sync command.
        # However, for a pure Python solution, consider exploring boto3's transfer module.
        output = subprocess.check_output(["aws", "s3", "sync", src_directory, s3_uri, "--delete"], stderr=subprocess.STDOUT, text=True)
        
        last_progress = None
        
        for line in output.splitlines():
            if not line.strip():  # Skip empty lines or lines with just spaces
                logger.warning("NOTICE - Incorrect data output format from the AWS CLI.")
                time.sleep(3)
                continue
            try:
                s3_path = line.split()[-1].replace(s3_uri + '/', '', 1)
            except IndexError as e:
                logger.error(f"IndexError: Unable to parse output: {str(e)}")
                time.sleep(3)
                continue

            if "upload" in line:
                logger.info(f"[Added] {line}")
            elif "delete" in line:
                logger.info(f"[Deleted] {line}")
            elif "Completed" in line:
                if "with ~0 file(s) remaining" in line:  # Only log the final progress update
                    logger.info(f"[Progress] {line}")
                # Skip other progress updates without logging them as unknown
            else:
                logger.warning(f"[Unknown Operation] {line}")
                sys.stdout.flush()
                time.sleep(2)
   
        sys.stdout.write(f"\rSynchronized {src_directory} to {s3_uri}.\n")
        sys.stdout.flush()
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"\rFailed to synchronize {src_directory} to {s3_uri}. Error: {str(e)}")
        time.sleep(3)
        return False
    except PermissionError:
        logger.error(f"Permission denied for {src_directory}. Skipping...")
        time.sleep(3)
        return False

def countdown_termination():
    print("This program will self-terminate in five seconds.")
    time.sleep(3)
    print("Exiting... ")
    for i in range(5, 0, -1):
        print(i)
        time.sleep(1)
    sys.exit()
    
def inactivity():
    inactive_timer = [60]  # use a mutable object so the nested function can modify it
    def timer():
        while inactive_timer[0] > 0:
            time.sleep(1)
            inactive_timer[0] -= 1
        if inactive_timer[0] <= 0:
            print("\nWARNING - Exiting due to inactivity... ")
            countdown_termination()

    t = threading.Thread(target=timer)
    t.start()

    while inactive_timer[0] > 0:
        print("Press y for yes or n for no.")
        command = input("Retry process? (y/n): ").lower()
        if not command:
            print("You must enter a valid command.")
            pause()
        elif command == "y":
            inactive_timer[0] = 0
            clear()
            logger.info(f"Retrying Auto Backup Process {src_directory}...")
            time.sleep(1)
            main()
        elif command == "n":
            inactive_timer[0] = 0
            countdown_termination()
            
def print_clear_line(msg):
    print(f"\033[K{msg}", end="\r", flush=True)

def count_files(directory):
    file_count = 0
    for root, dirs, files in os.walk(directory):
        file_count += len(files)
    return file_count

def calculate_total_files_to_process():
    total_files_to_process = 0
    for src in source_directories():
        src_file_count = count_files(src)
        total_files_to_process += src_file_count * 2  # Multiply by 2 because we process both src and dst
        print_clear_line(f"Processed files (counting): {total_files_to_process}")
    return total_files_to_process
            
def main():
    os_type = check_os()
    if os_type not in ["Windows", "mac", "Linux"]:
        logger.error(f"Unsupported OS: {os_type}")
        pause()
        countdown_termination()
    
    source_directories()
    
    print("Initializing Auto Backup To AWS S3. Stand by...")
    total_files_to_process = calculate_total_files_to_process()
    print(f"Total files to process: {total_files_to_process}")
    time.sleep(5)
    
    backup_successful = True

    for src_directory in source_directories():
        success = with_retry(sync_to_s3, src_directory, BUCKET_NAME)
        if not success:
            logger.error(f"Auto Backup {src_directory} unsuccessful.")
            backup_successful = False
            time.sleep(3)

    if backup_successful:       
        logger.info(f"Auto Backup completed on {time.strftime('%Y-%m-%d %H:%M:%S')}.")
    else:
        logger.error("ERROR - Backup failed during completion.")
        time.sleep(3)
        inactivity()
        
    countdown_termination()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:       
        print("NOTICE - Process interrupted by the user.")
        countdown_termination()