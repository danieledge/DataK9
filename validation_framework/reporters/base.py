"""Base reporter interface."""

from abc import ABC, abstractmethod
from validation_framework.core.results import ValidationReport


class Reporter(ABC):
    """Base class for report generators."""

    @abstractmethod
    def generate(self, report: ValidationReport, output_path: str):
        """
        Generate a report from validation results.

        Args:
            report: ValidationReport to generate from
            output_path: Path where report should be written
        """
        pass
