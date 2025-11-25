"""
Command-line interface for the Data Validation Framework.

Provides commands for:
- Running validations
- Listing available validation types
- Generating reports
"""

import click
import sys
from datetime import datetime
from pathlib import Path

from validation_framework.core.engine import ValidationEngine
from validation_framework.core.optimized_engine import OptimizedValidationEngine
from validation_framework.core.registry import get_registry
from validation_framework.core.logging_config import setup_logging, get_logger
from validation_framework.core.pretty_output import PrettyOutput as po
from validation_framework.utils.performance_advisor import get_performance_advisor
from validation_framework.utils.path_patterns import PathPatternExpander

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
              default='WARNING', help='Logging level')
@click.option('--log-file', type=click.Path(), help='Optional log file path')
@click.option('--no-optimize', is_flag=True, help='Disable single-pass optimization (use standard engine)')
def validate(config_file, html_output, json_output, verbose, fail_on_warning, log_level, log_file, no_optimize):
    """
    Run data validation from a configuration file.

    CONFIG_FILE: Path to YAML configuration file defining validations

    Output paths support date/time patterns:
    - {date} -> 2025-11-22
    - {time} -> 14-30-45
    - {timestamp} -> 20251122_143045
    - {datetime} -> 2025-11-22_14-30-45
    - {job_name} -> Job_Name (from config)

    Examples:

    \b
    # Basic validation
    data-validate validate config.yaml

    \b
    # With custom output paths
    data-validate validate config.yaml -o report.html -j results.json

    \b
    # With date/time patterns
    data-validate validate config.yaml -o "reports/{date}/validation_{time}.html"

    \b
    # Fail on warnings
    data-validate validate config.yaml --fail-on-warning

    \b
    # With custom log level and file
    data-validate validate config.yaml --log-level DEBUG --log-file "logs/{timestamp}.log"
    """
    # Create pattern expander with consistent timestamp for this run
    run_timestamp = datetime.now()
    expander = PathPatternExpander(run_timestamp=run_timestamp)

    # Expand log file pattern first (needed for setup_logging)
    if log_file:
        log_file = expander.expand(log_file, {})

    # Setup logging
    setup_logging(level=log_level, log_file=log_file)
    logger.info(f"Starting validation: {config_file}")
    logger.info(f"Log level: {log_level}")

    try:
        # Create and run validation engine (optimized by default)
        logger.debug(f"Loading configuration from {config_file}")
        if no_optimize:
            logger.info("Using standard validation engine (single-pass optimization disabled)")
            engine = ValidationEngine.from_config(config_file)
        else:
            logger.info("Using optimized validation engine (single-pass mode)")
            engine = OptimizedValidationEngine.from_config(config_file, use_single_pass=True)
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

        # Build context for pattern expansion
        context = {'job_name': engine.config.job_name}

        # Generate HTML report (expand patterns in CLI override)
        if html_output:
            html_output = expander.expand(html_output, context)
            engine.generate_html_report(report, html_output)
        else:
            # Use default from config (already expanded in config.py)
            html_path = engine.config.html_report_path
            engine.generate_html_report(report, html_path)

        # Generate JSON report (expand patterns in CLI override)
        if json_output:
            json_output = expander.expand(json_output, context)
            engine.generate_json_report(report, json_output)
        else:
            # Check if config specifies JSON output (already expanded in config.py)
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
@click.option('--html-output', '-o', help='Path for HTML profile report (default: {file_name}_profile_{date}.html)')
@click.option('--json-output', '-j', help='Path for JSON profile output')
@click.option('--config-output', '-c', help='Path to save generated validation config (default: {file_name}_validation_{timestamp}.yaml)')
@click.option('--chunk-size', type=int, default=None, help='Number of rows per chunk (default: auto-calculate based on available memory)')
@click.option('--sample', '-s', type=int, default=None, help='Profile only the first N rows (useful for quick analysis of large files)')
@click.option('--no-memory-check', is_flag=True, help='Disable memory usage warnings for large files')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='WARNING', help='Logging level')
@click.option('--disable-temporal', is_flag=True, help='Disable temporal analysis for datetime columns')
@click.option('--disable-pii', is_flag=True, help='Disable PII detection with privacy risk scoring')
@click.option('--disable-correlation', is_flag=True, help='Disable enhanced multi-method correlation analysis')
@click.option('--disable-all-enhancements', is_flag=True, help='Disable all profiler enhancements (temporal, PII, correlation)')
@click.option('--report-style', type=click.Choice(['classic', 'executive'], case_sensitive=False),
              default='executive', help='HTML report style: classic (detailed) or executive (dashboard view)')
def profile(file_path, format, database, table, query, html_output, json_output, config_output, chunk_size, sample, no_memory_check, log_level,
            disable_temporal, disable_pii, disable_correlation, disable_all_enhancements, report_style):
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

    Output paths support date/time patterns:
    - {date} -> 2025-11-22
    - {time} -> 14-30-45
    - {timestamp} -> 20251122_143045
    - {datetime} -> 2025-11-22_14-30-45
    - {file_name} -> source_file (from input file)
    - {table_name} -> table (from database table)

    Examples:

    \b
    # Profile a CSV file
    data-validate profile data/customers.csv

    \b
    # Profile with custom output paths
    data-validate profile data.csv -o profile.html -c validation.yaml

    \b
    # Profile with date/time patterns
    data-validate profile data.csv -o "profiles/{file_name}_{date}.html"

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
    from validation_framework.profiler.executive_html_reporter import ExecutiveHTMLReporter
    from validation_framework.loaders.factory import LoaderFactory

    # Create pattern expander with consistent timestamp for this run
    run_timestamp = datetime.now()
    expander = PathPatternExpander(run_timestamp=run_timestamp)

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
        # Handle --disable-all-enhancements flag (disables all Phase 1 features)
        if disable_all_enhancements:
            disable_temporal = True
            disable_pii = True
            disable_correlation = True

        # Initialize profiler with enhancements (enabled by default, disabled if flag set)
        profiler = DataProfiler(
            chunk_size=chunk_size,
            enable_temporal_analysis=not disable_temporal,
            enable_pii_detection=not disable_pii,
            enable_enhanced_correlation=not disable_correlation,
            disable_memory_safety=no_memory_check  # Pass through the --no-memory-check flag
        )

        # DATABASE MODE
        if database:
            logger.info(f"Starting profile of database: {database}")

            # Set default output paths based on table/query
            source_name = table if table else "query_result"
            context = {'table_name': source_name}

            if not html_output:
                html_output = f"{{table_name}}_profile_report_{{date}}.html"
            if not config_output:
                config_output = f"{{table_name}}_validation_{{timestamp}}.yaml"

            # Expand patterns
            html_output = expander.expand(html_output, context)
            config_output = expander.expand(config_output, context)

            click.echo(f"🔍 Profiling database: {table if table else 'query'}...")

            # Create database loader
            loader = LoaderFactory.create_database_loader(
                connection_string=database,
                table=table,
                query=query,
                chunk_size=chunk_size
            )

            # Get row count and load sample data
            row_count = loader.get_row_count()
            sample_chunk = next(loader.load())

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

            # Set default output paths with patterns
            file_stem = Path(file_path).stem
            context = {'file_name': file_stem}

            if not html_output:
                html_output = f"{{file_name}}_profile_report_{{date}}.html"
            if not config_output:
                config_output = f"{{file_name}}_validation_{{timestamp}}.yaml"

            # Expand patterns
            html_output = expander.expand(html_output, context)
            config_output = expander.expand(config_output, context)

            # Performance advisory: Recommend Parquet if large CSV (unless --no-memory-check specified)
            if not no_memory_check:
                advisor = get_performance_advisor()
                analysis = advisor.analyze_file(file_path, operation='profile')
                warnings_output = advisor.format_warnings_for_cli(analysis)
                if warnings_output:
                    click.echo("")  # Blank line
                    for line in warnings_output:
                        click.echo(line)
                    click.echo("")  # Blank line
            else:
                # Warning when memory checks are disabled
                click.echo("")
                click.echo("⚠️  WARNING: Memory checks disabled (--no-memory-check)")
                click.echo("    This flag disables memory safety checks and may cause system instability.")
                click.echo("    DO NOT use on production systems or shared infrastructure.")
                click.echo("    Only use for development/testing on dedicated hardware.")
                click.echo("")

            # Create profiler and run analysis
            if sample:
                click.echo(f"🔍 Profiling {file_path} (first {sample:,} rows)...")
            else:
                click.echo(f"🔍 Profiling {file_path}...")
            profile_result = profiler.profile_file(
                file_path=file_path,
                file_format=format,
                sample_rows=sample
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

        # Compact summary
        click.echo(f"\n✓ Profile complete: {profile_result.row_count:,} rows × {profile_result.column_count} cols | Quality: {profile_result.overall_quality_score:.0f}% | {profile_result.processing_time_seconds:.1f}s")

        # Generate HTML report (choose reporter based on style)
        if report_style and report_style.lower() == 'classic':
            reporter = ProfileHTMLReporter()
        else:
            reporter = ExecutiveHTMLReporter()
        reporter.generate_report(profile_result, html_output)
        click.echo(f"  → HTML: {html_output} ({report_style or 'executive'} style)")

        # Generate JSON output if requested
        if json_output:
            import json
            # Expand patterns in JSON output path
            context = {'file_name': Path(file_path).stem if file_path else (table or 'query_result')}
            json_output = expander.expand(json_output, context)

            with open(json_output, 'w') as f:
                json.dump(profile_result.to_dict(), f, indent=2)
            click.echo(f"  → JSON: {json_output}")

        # Save generated validation config
        if profile_result.generated_config_yaml:
            with open(config_output, 'w') as f:
                f.write(profile_result.generated_config_yaml)
            click.echo(f"  → Config: {config_output}")

        sys.exit(0)

    except FileNotFoundError as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Unexpected error: {str(e)}", err=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)


@cli.command('cda-analysis')
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--output', '-o', default='cda_gap_analysis_{timestamp}.html',
              help='Path for HTML gap analysis report (default: cda_gap_analysis_{timestamp}.html)')
@click.option('--json-output', '-j', help='Path for JSON gap analysis output')
@click.option('--fail-on-gaps', is_flag=True,
              help='Exit with error code if any gaps detected')
def cda_analysis(config_file, output, json_output, fail_on_gaps):
    """
    Analyze Critical Data Attribute (CDA) validation coverage.

    This command analyzes your validation configuration to detect gaps where
    Critical Data Attributes lack validation coverage.

    CONFIG_FILE: Path to YAML configuration file with critical_data_attributes defined

    Output paths support date/time patterns:
    - {date} -> 2025-11-22
    - {time} -> 14-30-45
    - {timestamp} -> 20251122_143045
    - {datetime} -> 2025-11-22_14-30-45
    - {job_name} -> Job_Name (from config)

    Examples:

    \b
    # Basic CDA gap analysis
    python3 -m validation_framework.cli cda-analysis config.yaml

    \b
    # With custom output path
    python3 -m validation_framework.cli cda-analysis config.yaml -o gaps.html

    \b
    # With date/time patterns
    python3 -m validation_framework.cli cda-analysis config.yaml -o "cda_reports/{job_name}_{date}.html"

    \b
    # Fail CI/CD if any gaps detected
    python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps

    \b
    # Generate JSON output for automation
    python3 -m validation_framework.cli cda-analysis config.yaml -j "cda_results/{timestamp}.json"
    """
    import yaml
    import json as json_module
    from validation_framework.cda import CDAGapAnalyzer, CDAReporter

    # Create pattern expander with consistent timestamp for this run
    run_timestamp = datetime.now()
    expander = PathPatternExpander(run_timestamp=run_timestamp)

    po.logo()
    po.header("CDA Gap Analysis")

    try:
        # Load configuration
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        # Check if CDAs are defined
        job_config = config.get('validation_job', config)
        cda_defs = job_config.get('critical_data_attributes', {})

        if not cda_defs:
            po.warning("No critical_data_attributes section found in configuration")
            po.info("Add critical_data_attributes to your YAML to enable CDA gap analysis")
            po.blank_line()
            po.info("Example:")
            po.info("  critical_data_attributes:")
            po.info("    customers:")
            po.info("      - field: customer_id")
            po.info("        description: Primary identifier")
            sys.exit(0)

        # Run analysis
        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        # Build context for pattern expansion
        context = {'job_name': report.job_name}

        # Expand patterns in output paths
        output = expander.expand(output, context)
        if json_output:
            json_output = expander.expand(json_output, context)

        # Display summary
        po.section("Analysis Summary")
        po.info(f"Job: {report.job_name}")
        po.info(f"Files analyzed: {len(report.results)}")
        po.blank_line()

        # Show per-file results
        for result in report.results:
            coverage_pct = result.coverage_percentage
            status = "✓" if not result.has_gaps else "✗"
            status_color = "green" if not result.has_gaps else "red"

            po.info(f"{status} {result.file_name}: {result.covered_cdas}/{result.total_cdas} covered ({coverage_pct:.0f}%)")

            if result.has_gaps:
                for fc in result.gaps:
                    po.warning(f"    ✗ {fc.cda.field} - No validation coverage")

        po.blank_line()

        # Summary box - expects list of (key, value, color) tuples
        summary_items = [
            ("Total CDAs", str(report.total_cdas), "cyan"),
            ("Covered", str(report.total_covered), "green"),
            ("Gaps", str(report.total_gaps), "red" if report.total_gaps > 0 else "green"),
            ("Coverage", f"{report.overall_coverage:.0f}%", "green" if report.overall_coverage >= 90 else "yellow")
        ]

        po.summary_box("CDA Coverage Summary", summary_items)

        # Generate HTML report
        reporter = CDAReporter()
        reporter.save_html(report, output)
        po.success(f"HTML report generated: {output}")

        # Generate JSON if requested
        if json_output:
            json_data = {
                'job_name': report.job_name,
                'timestamp': report.analysis_timestamp.isoformat(),
                'summary': {
                    'total_cdas': report.total_cdas,
                    'covered': report.total_covered,
                    'gaps': report.total_gaps,
                    'coverage_percentage': report.overall_coverage
                },
                'files': []
            }
            for result in report.results:
                file_data = {
                    'name': result.file_name,
                    'total_cdas': result.total_cdas,
                    'covered': result.covered_cdas,
                    'gaps': result.gap_cdas,
                    'coverage_percentage': result.coverage_percentage,
                    'fields': [
                        {
                            'field': fc.cda.field,
                            'is_covered': fc.is_covered,
                            'validations': fc.covering_validations,
                            'description': fc.cda.description
                        }
                        for fc in result.field_coverage
                    ]
                }
                json_data['files'].append(file_data)

            with open(json_output, 'w') as f:
                json_module.dump(json_data, f, indent=2)
            po.success(f"JSON report generated: {json_output}")

        # Determine exit code
        if report.has_gaps and fail_on_gaps:
            po.blank_line()
            po.error("CDA gaps detected - failing as requested")
            sys.exit(1)

        if report.has_gaps:
            po.blank_line()
            po.warning("CDA gaps detected - review recommended")
            sys.exit(0)

        po.blank_line()
        po.success("All Critical Data Attributes have validation coverage")
        sys.exit(0)

    except FileNotFoundError as e:
        po.error(f"File not found: {str(e)}")
        sys.exit(1)

    except Exception as e:
        po.error(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    cli()
