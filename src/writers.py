import datetime
import json
import logging
import os

from typing import List
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataTypeNotSupportedForIngestionException(Exception):
    
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)

class DataWriter(ABC):

    def __init__(self, api:str, page:int) -> None:
        self.api = api
        self.page = page
        self.filename = f"{self.api}/{self.page}.json"

    def _write_row(self, row: str) -> None:
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(self.filename, "a") as f:
            f.write(row)
    
    @abstractmethod
    def write(self):
        pass

class LocalDataWriter(DataWriter):
    type="local_data_writer"

    def write(self, data: [List, dict]):
        if isinstance(data, dict):
            self._write_row(json.dumps(data) + "\n")
        elif isinstance(data, List):
            for element in data:
                self.write(element)
        else:
            raise DataTypeNotSupportedForIngestionException(data)

class DataPublisher():
    type="data_publisher"
    
    def __init__(self, topic_arn: str, client) -> None:
        self.client = client
        self.topic_arn = topic_arn

    def _publish_event(self, event: dict):
        response = self.client.publish(
            TopicArn=f'{self.topic_arn}',
            Message=json.dumps(event) + "\n",
        )

        return response

    def write(self, data: [List, dict]):
        if isinstance(data, dict):
            response = self._publish_event(data)
            logger.info(f"SNS Publish Status: {response['ResponseMetadata']['HTTPStatusCode']}")

        elif isinstance(data, List):
            for element in data:
                response = self._publish_event(element)
                logger.info(f"SNS Publish Status: {response['ResponseMetadata']['HTTPStatusCode']}")
        else:
            raise DataTypeNotSupportedForIngestionException(data)