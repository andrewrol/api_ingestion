import time
import os
import boto3

from schedule import repeat, every, run_pending
from writers import LocalDataWriter
from writers import DataPublisher
from ingestors import UserListIngestor
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    # user_local_list_ingestor = UserListIngestor(
    #     writer=LocalDataWriter, 
    #     default_start_page=0)
    user_list_publisher_ingestor = UserListIngestor(
        writer=DataPublisher(
            topic_arn=os.getenv('USER_LIST_EVENT'), client=boto3.client('sns')
            ), 
        default_start_page=0)

@repeat(every(1).seconds)
def job():
    # user_local_list_ingestor.ingest()
    user_list_publisher_ingestor.ingest()

while True:
    run_pending()
    time.sleep(0.5)