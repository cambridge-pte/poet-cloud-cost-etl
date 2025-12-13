"""Base class for data sources."""

from abc import ABC, abstractmethod
from typing import Generator
import pandas as pd


class BaseSource(ABC):
    """Abstract base class for cloud cost data sources."""

    @abstractmethod
    def extract(self) -> Generator[pd.DataFrame, None, None]:
        """Extract data from the source.

        Yields DataFrames in chunks to handle large datasets.
        """
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the name of this source for logging."""
        pass
