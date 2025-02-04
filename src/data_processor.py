from abc import ABC, abstractmethod
from .logger import setup_logger
class DataProcessor(ABC):
    """Abstract base class for all data processing tasks."""

    def __init__(self, name='EcoPulse'):
        self.name = name
        self.logger = setup_logger(self.name)
        

    @abstractmethod
    def run(self, data):
        """Abstract method to be implemented by subclasses."""
        pass
