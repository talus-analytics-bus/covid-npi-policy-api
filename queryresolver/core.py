from abc import ABC, abstractmethod


class QueryResolver(ABC):
    def __init__(self):
        return None

    @abstractmethod
    def validate_args(self, **kwargs):
        """Validate input arguments and raise exception if error found"""
        pass
