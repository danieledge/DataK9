"""
Command-line interface for the Data Validation Framework.

Provides commands for:
- Running validations
- Listing available validation types
- Generating reports
"""

import click
import sys
from pathlib import Path

from validation_framework.core.engine import ValidationEngine
from validation_framework.core.registry import get_registry
from validation_framework.core.logging_config import setup_logging, get_logger
from validation_framework.core.pretty_output import PrettyOutput as po
from validation_framework.utils.performance_advisor import get_performance_advisor

logger = get_logger(__name__)


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """
    Data Validation Framework - Robust pre-load data quality checks.

    A comprehensive tool for validating data files before loading them
    into systems. Supports CSV, Excel, Parquet and validates data quality,
    completeness, schema conformance, and business rules.
    """
    pass


@cli.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--html-output', '-o', help='Path for HTML report output')
@click.option('--json-output', '-j', help='Path for JSON report output')
@click.option('--verbose/--quiet', '-v/-q', default=True, help='Verbose output')
@click.option('--fail-on-warning', is_flag=True, help='Fail if warnings are found')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='INFO', help='Logging level')
@click.option('--log-file', type=click.Path(), help='Optional log file path')
def validate(config_file, html_output, json_output, verbose, fail_on_warning, log_level, log_file):
    """
    Run data validation from a configuration file.

    CONFIG_FILE: Path to YAML configuration file defining validations

    Examples:

    \b
    # Basic validation
    data-validate validate config.yaml

    \b
    # With custom output paths
    data-validate validate config.yaml -o report.html -j results.json

    \b
    # Fail on warnings
    data-validate validate config.yaml --fail-on-warning

    \b
    # With custom log level and file
    data-validate validate config.yaml --log-level DEBUG --log-file validation.log
    """
    # Setup logging
    setup_logging(level=log_level, log_file=log_file)
    logger.info(f"Starting validation: {config_file}")
    logger.info(f"Log level: {log_level}")

    try:
        # Create and run validation engine
        logger.debug(f"Loading configuration from {config_file}")
        engine = ValidationEngine.from_config(config_file)
        logger.info(f"Configuration loaded: {engine.config.job_name}")

        # Performance advisory: Check files and recommend Parquet if needed
        # (Skip database sources)
        advisor = get_performance_advisor()
        for file_config in engine.config.files:
            # Skip database sources - performance advisor is for files only
            if file_config.get("format") == "database":
                continue

            file_path = file_config["path"]
            if Path(file_path).exists():
                analysis = advisor.analyze_file(file_path, operation='validation')
                warnings_output = advisor.format_warnings_for_cli(analysis)
                if warnings_output:
                    for line in warnings_output:
                        po.info(line)
                    po.blank_line()

        report = engine.run(verbose=verbose)

        # Generate HTML report
        if html_output:
            engine.generate_html_report(report, html_output)
        else:
            # Use default from config or fallback
            html_path = engine.config.html_report_path
            engine.generate_html_report(report, html_path)

        # Generate JSON report
        if json_output:
            engine.generate_json_report(report, json_output)
        else:
            # Check if config specifies JSON output
            if engine.config.json_summary_path:
                engine.generate_json_report(report, engine.config.json_summary_path)

        # Determine exit code based on results
        if report.has_errors():
            if engine.config.fail_on_error:
                po.blank_line()
                po.error("VALIDATION FAILED WITH ERRORS")
                po.info(f"HTML Report: {engine.config.html_report_path}")
                sys.exit(1)

        if report.has_warnings() and (fail_on_warning or engine.config.fail_on_warning):
            po.blank_line()
            po.warning("Validation completed with warnings (treating as failure)")
            po.info(f"HTML Report: {engine.config.html_report_path}")
            sys.exit(2)

        if report.has_errors() or report.has_warnings():
            po.blank_line()
            po.warning("Validation completed with issues (warnings only)")
            po.info(f"HTML Report: {engine.config.html_report_path}")
            sys.exit(0)

        po.blank_line()
        po.success("VALIDATION PASSED")
        po.info(f"HTML Report: {engine.config.html_report_path}")
        sys.exit(0)

    except FileNotFoundError as e:
        po.blank_line()
        po.error(f"File not found: {str(e)}")
        sys.exit(1)

    except Exception as e:
        po.blank_line()
        po.error(f"Unexpected error: {str(e)}")
        if verbose:
            import traceback
            po.blank_line()
            traceback.print_exc()
        sys.exit(1)


@cli.command()
@click.option('--category', '-c', type=click.Choice(['all', 'file', 'schema', 'field', 'record']),
              default='all', help='Filter by validation category')
@click.option('--source', '-s', type=click.Choice(['file', 'database']),
              help='Filter by source compatibility (file or database)')
@click.option('--show-compatibility', is_flag=True,
              help='Show source compatibility for each validation')
def list_validations(category, source, show_compatibility):
    """
    List all available validation types.

    Use --category to filter by validation category:
    - file: File-level checks (empty files, row counts, etc.)
    - schema: Schema validation (columns, types, etc.)
    - field: Field-level checks (mandatory, regex, ranges, etc.)
    - record: Record-level checks (duplicates, blanks, etc.)

    Use --source to filter by source compatibility:
    - file: Validations that work with file sources
    - database: Validations that work with database sources

    Examples:

    \b
    # List all validations
    data-validate list-validations

    \b
    # List only field-level validations
    data-validate list-validations --category field

    \b
    # List validations that work with databases
    data-validate list-validations --source database

    \b
    # Show source compatibility for all validations
    data-validate list-validations --show-compatibility
    """
    from validation_framework.utils.definition_loader import ValidationDefinitionLoader
    from pathlib import Path

    registry = get_registry()

    # Create fresh loader to avoid singleton cache issues
    # Get path relative to this file
    cli_dir = Path(__file__).parent
    def_file = cli_dir / "validation_definitions.json"
    definition_loader = ValidationDefinitionLoader(def_file)

    # Get validations from registry
    validations = sorted(registry.list_available())

    # Filter by source compatibility if specified
    if source:
        compatible = definition_loader.get_by_source_compatibility(source)
        validations = [v for v in validations if v in compatible]

    # Category filtering (simple string matching)
    if category != 'all':
        category_keywords = {
            'file': ['file', 'size', 'row'],
            'schema': ['schema', 'column'],
            'field': ['field', 'mandatory', 'regex', 'values', 'range', 'date', 'format'],
            'record': ['duplicate', 'blank', 'unique', 'record'],
        }
        keywords = category_keywords.get(category, [])
        validations = [v for v in validations if any(k.lower() in v.lower() for k in keywords)]

    # Show header with filter info
    filter_info = []
    if category != 'all':
        filter_info.append(f"category={category}")
    if source:
        filter_info.append(f"source={source}")
    filter_str = f" ({', '.join(filter_info)})" if filter_info else ""

    click.echo(f"\nAvailable Validations{filter_str}: {len(validations)}\n")

    # Show compatibility summary if requested
    if show_compatibility and not source:
        summary = definition_loader.get_compatibility_summary()
        click.echo("📊 Source Compatibility Summary:")
        click.echo(f"   Total validations: {summary['total']}")
        click.echo(f"   📁 File-compatible: {summary['file_compatible']}")
        click.echo(f"   🗄️  Database-compatible: {summary['database_compatible']}")
        click.echo(f"   Both: {summary['both_compatible']}")
        click.echo()

    for validation in validations:
        try:
            # Get source compatibility badges
            compat = definition_loader.get_source_compatibility(validation)
            badges = []
            if compat.get('file'):
                badges.append('📁')
            if compat.get('database'):
                badges.append('🗄️')
            badge_str = ' '.join(badges) if (show_compatibility or source) else ''

            # Get validation class to show description
            validation_class = registry.get(validation)
            # Create temporary instance to get description
            from validation_framework.core.results import Severity
            instance = validation_class(name=validation, severity=Severity.ERROR, params={})
            description = instance.get_description()

            # Format output
            name_with_badge = f"{badge_str} {validation}" if badge_str else f"  • {validation}"
            click.echo(name_with_badge)
            click.echo(f"    {description}")

            # Show compatibility notes if available
            if show_compatibility and compat.get('notes'):
                click.echo(f"    💡 {compat['notes']}")

            click.echo()
        except Exception:
            click.echo(f"  • {validation}\n")


@cli.command()
@click.argument('output_path', type=click.Path())
def init_config(output_path):
    """
    Generate a sample configuration file.

    OUTPUT_PATH: Path where sample config should be written

    Example:

    \b
    data-validate init-config my_validation.yaml
    """
    sample_config = '''# Data Validation Configuration
# Generated by Data Validation Framework

validation_job:
  name: "Sample Data Validation"
  version: "1.0"

  files:
    # Example CSV file validation
    - name: "customers"
      path: "data/customers.csv"
      format: "csv"
      delimiter: ","
      encoding: "utf-8"

      validations:
        # File-level checks
        - type: "EmptyFileCheck"
          severity: "ERROR"

        - type: "RowCountRangeCheck"
          severity: "WARNING"
          params:
            min_rows: 100
            max_rows: 1000000

        # Schema validation
        - type: "SchemaMatchCheck"
          severity: "ERROR"
          params:
            expected_schema:
              customer_id: "integer"
              name: "string"
              email: "string"
              balance: "float"
              created_date: "date"

        # Field-level validations
        - type: "MandatoryFieldCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id", "name", "email"]

        - type: "RegexCheck"
          severity: "ERROR"
          params:
            field: "email"
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$"
            message: "Invalid email format"

        - type: "RangeCheck"
          severity: "WARNING"
          params:
            field: "balance"
            min_value: 0
            max_value: 1000000

        # Record-level checks
        - type: "DuplicateRowCheck"
          severity: "ERROR"
          params:
            key_fields: ["customer_id"]

  # Output configuration
  output:
    html_report: "validation_report.html"
    json_summary: "validation_summary.json"
    fail_on_error: true
    fail_on_warning: false

  # Processing options
  processing:
    chunk_size: 50000  # Rows per chunk (for large files)
    parallel_files: false
    max_sample_failures: 100
'''

    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w') as f:
            f.write(sample_config)

        click.echo(f"✓ Sample configuration written to: {output_path}")
        click.echo(f"\nEdit the file to customize for your data, then run:")
        click.echo(f"  data-validate validate {output_path}")

    except Exception as e:
        click.echo(f"❌ Error creating config file: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
def version():
    """Display version information."""
    click.echo("Data Validation Framework v0.1.0")
    click.echo("A robust tool for pre-load data quality validation")


@cli.command()
@click.argument('file_path', type=click.Path(exists=True), required=False)
@click.option('--format', '-f', type=click.Choice(['csv', 'excel', 'json', 'parquet'], case_sensitive=False),
              help='File format (auto-detected if not specified)')
@click.option('--database', '--db', help='Database connection string (e.g., sqlite:///test.db or postgresql://...)')
@click.option('--table', '-t', help='Database table name to profile')
@click.option('--query', '-q', help='SQL query to profile (alternative to --table)')
@click.option('--html-output', '-o', help='Path for HTML profile report (default: profile_report.html)')
@click.option('--json-output', '-j', help='Path for JSON profile output')
@click.option('--config-output', '-c', help='Path to save generated validation config (default: <filename>_validation.yaml)')
@click.option('--chunk-size', type=int, default=50000, help='Number of rows per chunk for large files/tables')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='INFO', help='Logging level')
def profile(file_path, format, database, table, query, html_output, json_output, config_output, chunk_size, log_level):
    """
    Profile a data file or database table to understand its structure and quality.

    Generates a comprehensive analysis including:
    - Schema and data type inference (known vs inferred)
    - Statistical distributions and patterns
    - Data quality metrics
    - Correlations between fields
    - Suggested validations
    - Auto-generated validation configuration

    FILE_PATH: Path to data file to profile (not required if using --database)

    Examples:

    \b
    # Profile a CSV file
    data-validate profile data/customers.csv

    \b
    # Profile with custom output paths
    data-validate profile data.csv -o profile.html -c validation.yaml

    \b
    # Profile large Parquet file with custom chunk size
    data-validate profile large_data.parquet --chunk-size 100000

    \b
    # Profile a database table
    data-validate profile --database "sqlite:///test.db" --table customers

    \b
    # Profile database with custom query
    data-validate profile --db "postgresql://user:pass@localhost/db" --query "SELECT * FROM orders WHERE date > '2024-01-01'"
    """
    from validation_framework.profiler.engine import DataProfiler
    from validation_framework.profiler.html_reporter import ProfileHTMLReporter
    from validation_framework.loaders.factory import LoaderFactory

    # Setup logging
    setup_logging(level=log_level)

    # Validate arguments
    if not file_path and not database:
        click.echo("❌ Error: Must provide either FILE_PATH or --database option", err=True)
        click.echo("Run 'data-validate profile --help' for usage examples")
        sys.exit(1)

    if database and not (table or query):
        click.echo("❌ Error: When using --database, must provide either --table or --query", err=True)
        sys.exit(1)

    if file_path and database:
        click.echo("❌ Error: Cannot use both FILE_PATH and --database. Choose one.", err=True)
        sys.exit(1)

    try:
        profiler = DataProfiler(chunk_size=chunk_size)

        # DATABASE MODE
        if database:
            logger.info(f"Starting profile of database: {database}")

            # Set default output paths based on table/query
            source_name = table if table else "query_result"
            if not html_output:
                html_output = f"{source_name}_profile_report.html"
            if not config_output:
                config_output = f"{source_name}_validation.yaml"

            click.echo(f"🗄️  Profiling database table: {table if table else 'custom query'}...")

            # Create database loader
            loader = LoaderFactory.create_database_loader(
                connection_string=database,
                table=table,
                query=query,
                chunk_size=chunk_size
            )

            # Get row count
            row_count = loader.get_row_count()
            click.echo(f"  • Detected {row_count:,} rows in {source_name}")

            # Load sample data for profiling
            click.echo(f"  • Loading sample data...")
            sample_chunk = next(loader.load_chunks())

            # Profile the sample
            profile_result = profiler.profile_dataframe(sample_chunk, name=source_name)

            # Update metadata for database source
            profile_result.total_rows = row_count
            profile_result.source_type = "database"
            profile_result.file_name = f"{source_name} (Database)"

        # FILE MODE
        else:
            logger.info(f"Starting profile of: {file_path}")

            # Auto-detect format if not specified
            if not format:
                file_ext = Path(file_path).suffix.lower()
                format_map = {
                    '.csv': 'csv',
                    '.xlsx': 'excel',
                    '.xls': 'excel',
                    '.json': 'json',
                    '.jsonl': 'json',
                    '.parquet': 'parquet'
                }
                format = format_map.get(file_ext, 'csv')
                logger.info(f"Auto-detected format: {format}")

            # Set default output paths
            file_stem = Path(file_path).stem
            if not html_output:
                html_output = f"{file_stem}_profile_report.html"
            if not config_output:
                config_output = f"{file_stem}_validation.yaml"

            # Performance advisory: Recommend Parquet if large CSV
            advisor = get_performance_advisor()
            analysis = advisor.analyze_file(file_path, operation='profile')
            warnings_output = advisor.format_warnings_for_cli(analysis)
            if warnings_output:
                click.echo("")  # Blank line
                for line in warnings_output:
                    click.echo(line)
                click.echo("")  # Blank line

            # Create profiler and run analysis
            click.echo(f"🔍 Profiling {file_path}...")
            profile_result = profiler.profile_file(
                file_path=file_path,
                file_format=format
            )

        # Format file size
        size_bytes = profile_result.file_size_bytes
        if size_bytes < 1024:
            size_str = f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            size_str = f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            size_str = f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            size_str = f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

        click.echo(f"\n📊 Profile Summary:")
        click.echo(f"  • File: {profile_result.file_name}")
        click.echo(f"  • Size: {size_str}")
        click.echo(f"  • Rows: {profile_result.row_count:,}")
        click.echo(f"  • Columns: {profile_result.column_count}")
        click.echo(f"  • Overall Quality Score: {profile_result.overall_quality_score:.1f}%")
        click.echo(f"  • Processing Time: {profile_result.processing_time_seconds:.2f}s")

        # Generate HTML report
        reporter = ProfileHTMLReporter()
        reporter.generate_report(profile_result, html_output)
        click.echo(f"\n✅ HTML report generated: {html_output}")

        # Generate JSON output if requested
        if json_output:
            import json
            with open(json_output, 'w') as f:
                json.dump(profile_result.to_dict(), f, indent=2)
            click.echo(f"✅ JSON output saved: {json_output}")

        # Save generated validation config
        if profile_result.generated_config_yaml:
            with open(config_output, 'w') as f:
                f.write(profile_result.generated_config_yaml)
            click.echo(f"✅ Validation config saved: {config_output}")
            click.echo(f"\n💡 To run validations, use:")
            click.echo(f"   {profile_result.generated_config_command}")

        # Show top suggestions
        if profile_result.suggested_validations:
            click.echo(f"\n💡 Top Validation Suggestions:")
            for sugg in profile_result.suggested_validations[:5]:
                click.echo(f"  • {sugg.validation_type} ({sugg.severity})")
                click.echo(f"    {sugg.reason}")

        sys.exit(0)

    except FileNotFoundError as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Unexpected error: {str(e)}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    cli()
