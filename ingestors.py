import datetime

from abc import ABC, abstractmethod
from typing import List
from writers import DataWriter
from apis import UserListApi

class DataIngestor(ABC):

    def __init__(self, writer: DataWriter, default_start_page: int):
        self.default_start_page = default_start_page
        self.writer = writer
        self._checkpoint = self._load_checkpoint()

    @property
    def _checkpoint_filesname(self) -> str:
        return f"{self.__class__.__name__}.checkpoint"

    def _write_checkpoint(self):
        with open(self._checkpoint_filesname, "w") as f:
            f.write(f"{self._checkpoint}")

    def _load_checkpoint(self) -> datetime:
        try:
            with open(self._checkpoint_filesname, "r") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def _get_checkpoint(self):
        if not self._checkpoint:
            return self.default_start_page
        else:
            return self._checkpoint

    def _update_checkpoint(self, value):
        self._checkpoint = value
        self._write_checkpoint()

    @abstractmethod
    def ingest(self) -> None:
        pass

class UserListIngestor(DataIngestor):
    def __init__(self, writer, default_start_page: int):
        super().__init__(writer, default_start_page)
        self.writer = writer

    def _transform_data(self, data):
        data = data['data']
        
        return data

    def _check_writer_and_write(self, writer, data, api=None, page=None):

        if writer.type == "data_publisher":
            self.writer.write(data)
        elif writer.type == "local_data_writer":
            self.writer(api=api.type, page=page).write(data)

    def ingest(self) -> None:
        page = int(self._get_checkpoint())
        if page < 5:
            api = UserListApi()
            data = api.get_data(page=page)
            data = self._transform_data(data=data)
            self._check_writer_and_write(writer=self.writer, data=data)
            self._update_checkpoint(page + 1)