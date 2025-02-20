from abc import ABC, abstractmethod
from typing import Any, Optional

from .logger import setup_logger


class DataProcessor(ABC):
    """Abstract base class for all data processing tasks."""

    def __init__(self, name="EcoPulse"):
        self.name = name
        self._logger = setup_logger(self.name)

    @abstractmethod
    def run(self, data: Optional[Any] = None) -> Any:
        """Abstract method to be implemented by subclasses."""
        pass
