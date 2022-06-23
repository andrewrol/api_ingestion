import requests
import logging
import ratelimit
import os

from abc import ABC, abstractmethod
from backoff import on_exception, expo
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DummyApi(ABC):

    def __init__(self) -> None:
        self.APP_ID = os.getenv('APP_ID')
        self.base_endpoint = 'https://dummyapi.io/data/v1'


    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        pass


    @on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
    @ratelimit.limits(calls=29, period=30)
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self, **kwargs) -> dict:
        endpoint = self._get_endpoint(**kwargs)
        logger.info(f"Getting data from endpoint: {endpoint}")
        response = requests.get(endpoint, headers={"app-id":f"{self.APP_ID}"})
        response.raise_for_status()
        return response.json()

class UserListApi(DummyApi):
    type = "user"

    def _get_endpoint(self, page: int = None) -> str:
        
        if page:
            endpoint = f'{self.base_endpoint}/{self.type}?page={page}'
        else:
            endpoint = f'{self.base_endpoint}/{self.type}?page=0'
    
        return endpoint