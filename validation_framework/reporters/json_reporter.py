"""
JSON report generator.

Generates machine-readable JSON reports suitable for:
- Programmatic parsing
- Integration with other systems
- Archiving validation results
- API responses
"""

import json
from pathlib import Path
from validation_framework.reporters.base import Reporter
from validation_framework.core.results import ValidationReport


class JSONReporter(Reporter):
    """
    Generates JSON format validation reports.

    The JSON report contains the complete validation results in a
    structured format suitable for programmatic processing.
    """

    def generate(self, report: ValidationReport, output_path: str):
        """
        Generate JSON report from validation results.

        Args:
            report: ValidationReport with validation results
            output_path: Path where JSON file should be written

        Raises:
            IOError: If unable to write report file
        """
        try:
            # Convert report to dictionary
            report_dict = report.to_dict()

            # Ensure output directory exists
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Write JSON file
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report_dict, f, indent=2, default=str)

            print(f"JSON report generated: {output_path}")

        except Exception as e:
            raise IOError(f"Error generating JSON report: {str(e)}")
