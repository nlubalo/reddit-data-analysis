from abc import ABC, abstractmethod

class BaseReader(ABC):
    @abstractmethod
    def authenticate(self, path: str):
        return NotImplementedError

    @abstractmethod
    def run_query(self):
        return NotImplementedError
    