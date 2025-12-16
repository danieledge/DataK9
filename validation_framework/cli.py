"""
Command-line interface for the Data Validation Framework.

Provides commands for:
- Running validations
- Listing available validation types
- Generating reports

Exit Codes:
- 0: Success
- 1: Validation failed (data quality errors)
- 2: Warnings treated as failure (--fail-on-warning)
- 3: Timeout exceeded (--timeout)
- 4: Lock file conflict (--lock-file)
- 5: Environment error (missing dependencies, wrong Python version)
- 130: Interrupted (SIGINT/SIGTERM)
- 137: Memory limit exceeded

Platform Support:
- Linux/macOS: Full feature support (file locking, timeouts, signals)
- Windows: Partial support (no file locking or timeouts, Ctrl+C only)
"""

import sys
import platform

# Platform detection for cross-platform compatibility
IS_WINDOWS = platform.system() == 'Windows'

# =============================================================================
# DEPENDENCY CHECK - Graceful handling of missing dependencies
# =============================================================================
# This runs before any other imports to catch missing dependencies early
# and provide helpful error messages instead of ugly tracebacks.

REQUIRED_PACKAGES = {
    'click': 'click',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'yaml': 'pyyaml',
    'colorama': 'colorama',
}

def _check_dependencies():
    """Check that all required dependencies are installed."""
    missing = []
    for module_name, package_name in REQUIRED_PACKAGES.items():
        try:
            __import__(module_name)
        except ImportError:
            missing.append(package_name)
    return missing

def _check_python_version():
    """Check Python version is 3.8+."""
    if sys.version_info < (3, 8):
        return f"Python 3.8+ required, found {sys.version_info.major}.{sys.version_info.minor}"
    return None

# Run checks immediately on import
_missing_deps = _check_dependencies()
_python_error = _check_python_version()

if _python_error or _missing_deps:
    # Print error without fancy formatting (colorama may not be available)
    print("\n" + "=" * 60, file=sys.stderr)
    print("ERROR: DataK9 Environment Check Failed", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    if _python_error:
        print(f"\n  Python Version: {_python_error}", file=sys.stderr)

    if _missing_deps:
        print(f"\n  Missing Dependencies:", file=sys.stderr)
        for pkg in _missing_deps:
            print(f"    - {pkg}", file=sys.stderr)
        print(f"\n  Install with: pip install {' '.join(_missing_deps)}", file=sys.stderr)

    print("\n" + "=" * 60, file=sys.stderr)
    print("Exit Code: 5 (Environment Error)", file=sys.stderr)
    print("=" * 60 + "\n", file=sys.stderr)
    sys.exit(5)

# =============================================================================
# STANDARD IMPORTS - Only run if dependencies are available
# =============================================================================

import click
import csv
import os
from datetime import datetime
from pathlib import Path

try:
    from validation_framework.core.engine import ValidationEngine
    from validation_framework.core.optimized_engine import OptimizedValidationEngine
    from validation_framework.core.registry import get_registry
    from validation_framework.core.logging_config import setup_logging, get_logger
    from validation_framework.core.pretty_output import PrettyOutput as po, VerboseProgressReporter
    from validation_framework.utils.performance_advisor import get_performance_advisor
    from validation_framework.utils.path_patterns import PathPatternExpander
except ImportError as e:
    # Internal module import failed - installation is broken
    print("\n" + "=" * 60, file=sys.stderr)
    print("ERROR: DataK9 Installation Corrupted", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"\n  Import Error: {e}", file=sys.stderr)
    print("\n  Try reinstalling: pip install --force-reinstall datak9", file=sys.stderr)
    print("\n" + "=" * 60, file=sys.stderr)
    print("Exit Code: 5 (Environment Error)", file=sys.stderr)
    print("=" * 60 + "\n", file=sys.stderr)
    sys.exit(5)

logger = get_logger(__name__)


def detect_csv_delimiter(file_path: str, sample_size: int = 8192) -> str:
    """
    Auto-detect the delimiter used in a CSV file.

    Uses Python's csv.Sniffer to analyze a sample of the file.
    Returns the detected delimiter or ',' as default.
    """
    # Try multiple encodings (Windows often uses cp1252)
    encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1']

    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as f:
                sample = f.read(sample_size)

            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=',\t|;:')
            return dialect.delimiter
        except (UnicodeDecodeError, csv.Error):
            continue
        except Exception:
            break

    # Fall back to comma if detection fails
    return ','


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
@click.option('--delimiter', '-d', default=None, help='Column delimiter for CSV files (overrides config). Use "\\t" for tab.')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False),
              default='WARNING', help='Logging level')
@click.option('--log-file', type=click.Path(), help='Optional log file path')
@click.option('--no-optimize', is_flag=True, help='Disable single-pass optimization (use standard engine)')
@click.option('--timeout', type=int, default=0, help='Timeout in seconds (0=no timeout). For batch/Autosys scheduling.')
@click.option('--lock-file', type=click.Path(), help='Lock file to prevent concurrent runs. For batch/Autosys.')
@click.option('--exit-file', type=click.Path(), help='Write exit code to file on completion. For batch/Autosys.')
def validate(config_file, html_output, json_output, verbose, fail_on_warning, delimiter, log_level, log_file, no_optimize, timeout, lock_file, exit_file):
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

    \b
    # Batch/Autosys mode with timeout and lock file
    data-validate validate config.yaml --timeout 3600 --lock-file /tmp/validation.lock

    Exit Codes:
      0 = Passed (no errors, warnings only if --fail-on-warning not set)
      1 = Failed (validation errors or unexpected errors)
      2 = Warnings treated as failure (--fail-on-warning set)
      3 = Timeout exceeded
      4 = Lock file conflict (another instance running)
      5 = Environment error (missing dependencies, wrong Python version)
      130 = Interrupted (Ctrl+C / SIGINT)
      137 = Memory limit exceeded
    """
    import signal
    import threading

    # Platform-specific imports for file locking
    if IS_WINDOWS:
        # Windows: Limited support - no file locking, no SIGALRM
        fcntl = None
    else:
        # Unix/Linux/macOS: Full support
        import fcntl

    # Helper to write exit code to file (for batch systems)
    def write_exit_code(code: int):
        if exit_file:
            try:
                Path(exit_file).parent.mkdir(parents=True, exist_ok=True)
                Path(exit_file).write_text(str(code))
            except Exception as e:
                logger.warning(f"Failed to write exit file: {e}")

    # Helper for clean exit with exit file writing
    def clean_exit(code: int):
        write_exit_code(code)
        sys.exit(code)

    # Helper to release lock file (used in multiple exception handlers)
    def release_lock():
        nonlocal lock_fd
        if lock_fd and not IS_WINDOWS:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
                os.unlink(lock_file)
            except Exception:
                pass
            lock_fd = None

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

    # Lock file handling for batch/Autosys (prevent concurrent runs)
    # Note: Full locking only supported on Unix/Linux/macOS
    lock_fd = None
    if lock_file:
        if IS_WINDOWS:
            # Windows: Lock files not supported, warn user
            po.warning("Lock files (--lock-file) are not supported on Windows")
            po.info("For Windows batch processing, use external job scheduling to prevent concurrent runs")
        else:
            try:
                Path(lock_file).parent.mkdir(parents=True, exist_ok=True)
                lock_fd = open(lock_file, 'w')
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_fd.write(f"{os.getpid()}\n{datetime.now().isoformat()}\n")
                lock_fd.flush()
                logger.info(f"Acquired lock file: {lock_file}")
            except BlockingIOError:
                po.error(f"Another validation instance is running (lock file: {lock_file})")
                po.info("If this is incorrect, delete the lock file and retry")
                clean_exit(4)
            except Exception as e:
                po.warning(f"Could not create lock file: {e}")

    # Timeout handling for batch/Autosys
    # Note: Uses SIGALRM on Unix, threading.Timer on Windows
    timeout_timer = None

    def timeout_handler_unix(signum, frame):
        """Unix signal-based timeout handler."""
        po.progress_done()
        po.blank_line()
        po.error(f"Validation timed out after {timeout} seconds")
        logger.error(f"Validation timed out after {timeout} seconds")
        release_lock()
        clean_exit(3)

    def timeout_handler_windows():
        """Windows threading-based timeout handler."""
        po.progress_done()
        po.blank_line()
        po.error(f"Validation timed out after {timeout} seconds")
        logger.error(f"Validation timed out after {timeout} seconds")
        # Force exit since we're in a different thread
        os._exit(3)

    if timeout > 0:
        if IS_WINDOWS:
            # Windows: Use threading.Timer (less reliable but functional)
            timeout_timer = threading.Timer(timeout, timeout_handler_windows)
            timeout_timer.daemon = True
            timeout_timer.start()
            logger.info(f"Timeout set: {timeout} seconds (Windows timer)")
        else:
            # Unix: Use SIGALRM (reliable)
            signal.signal(signal.SIGALRM, timeout_handler_unix)
            signal.alarm(timeout)
            logger.info(f"Timeout set: {timeout} seconds")

    # Signal handling for graceful shutdown
    def signal_handler(signum, frame):
        signal_name = signal.Signals(signum).name
        po.progress_done()
        po.blank_line()
        po.warning(f"Received {signal_name}, shutting down gracefully...")
        logger.warning(f"Received {signal_name}, shutting down")
        # Cancel timeout timer if active (Windows)
        if timeout_timer and timeout_timer.is_alive():
            timeout_timer.cancel()
        release_lock()
        clean_exit(130)

    # SIGTERM works on both Unix and Windows
    signal.signal(signal.SIGTERM, signal_handler)

    # Create progress reporter for verbose mode
    reporter = VerboseProgressReporter(verbose=verbose)

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

        # Override delimiter for all files if specified on CLI
        if delimiter:
            delim_char = delimiter.encode().decode('unicode_escape')
            for file_config in engine.config.files:
                file_config['delimiter'] = delim_char
            logger.info(f"Using delimiter: {repr(delim_char)}")

        # Performance advisory: Check files and recommend Parquet if needed
        # (Skip database sources)
        advisor = get_performance_advisor()
        for file_config in engine.config.files:
            # Skip database sources - performance advisor is for files only
            if file_config.get("format") == "database":
                continue

            file_path_check = file_config["path"]
            if Path(file_path_check).exists():
                analysis = advisor.analyze_file(file_path_check, operation='validation')
                warnings_output = advisor.format_warnings_for_cli(analysis)
                if warnings_output:
                    for line in warnings_output:
                        po.info(line)
                    po.blank_line()

        report = engine.run(verbose=verbose, progress_callback=reporter.callback)

        # Cancel timeout on successful completion
        if timeout > 0:
            if IS_WINDOWS:
                if timeout_timer and timeout_timer.is_alive():
                    timeout_timer.cancel()
            else:
                signal.alarm(0)

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

        # Release lock file
        if lock_fd and not IS_WINDOWS:
            logger.info(f"Releasing lock file: {lock_file}")
        release_lock()

        # Log summary for headless/batch mode
        logger.info(f"Validation complete: Errors={report.total_errors}, Warnings={report.total_warnings}")
        logger.info(f"Duration: {report.duration_seconds:.2f}s")
        logger.info(f"HTML Report: {engine.config.html_report_path}")

        # Determine exit code based on results
        if report.has_errors():
            if engine.config.fail_on_error:
                po.blank_line()
                po.error("VALIDATION FAILED WITH ERRORS")
                po.info(f"HTML Report: {engine.config.html_report_path}")
                po.key_value("Errors", report.total_errors, indent=2, value_color=po.ERROR)
                po.key_value("Warnings", report.total_warnings, indent=2, value_color=po.WARNING if report.total_warnings > 0 else po.DIM)
                clean_exit(1)

        if report.has_warnings() and (fail_on_warning or engine.config.fail_on_warning):
            po.blank_line()
            po.warning("Validation completed with warnings (treating as failure)")
            po.info(f"HTML Report: {engine.config.html_report_path}")
            po.key_value("Warnings", report.total_warnings, indent=2, value_color=po.WARNING)
            clean_exit(2)

        if report.has_errors() or report.has_warnings():
            po.blank_line()
            po.warning("Validation completed with issues (warnings only)")
            po.info(f"HTML Report: {engine.config.html_report_path}")
            po.key_value("Warnings", report.total_warnings, indent=2, value_color=po.WARNING)
            clean_exit(0)

        po.blank_line()
        po.success("VALIDATION PASSED")
        po.info(f"HTML Report: {engine.config.html_report_path}")
        clean_exit(0)

    except KeyboardInterrupt:
        po.progress_done()
        po.blank_line()
        po.warning("Validation interrupted by user")
        logger.warning("Validation interrupted by user (Ctrl+C)")
        release_lock()
        clean_exit(130)

    except MemoryError as e:
        po.progress_done()
        po.blank_line()
        po.error("Memory limit exceeded - validation terminated")
        error_msg = str(e)
        if error_msg:
            click.echo(f"   {error_msg}", err=True)
        logger.error(f"Memory limit exceeded: {e}")
        po.blank_line()
        po.info("Solutions:")
        click.echo("   1. Process smaller files or sample data")
        click.echo("   2. Use smaller chunk size in config: chunk_size: 10000")
        click.echo("   3. Close other applications to free memory")
        click.echo("   4. Convert large CSV files to Parquet format")
        release_lock()
        clean_exit(137)

    except FileNotFoundError as e:
        po.blank_line()
        po.error(f"File not found: {str(e)}")
        logger.error(f"File not found: {e}")
        release_lock()
        clean_exit(1)

    except RuntimeError as e:
        # Graceful error from loaders (CSV parsing, encoding issues)
        error_msg = str(e)
        po.blank_line()
        po.error("Error processing file:")
        click.echo(f"   {error_msg}", err=True)
        logger.error(f"Runtime error: {error_msg}")
        if "delimiter" in error_msg.lower():
            po.blank_line()
            po.info("Tip: Try specifying the delimiter with -d option or in config:")
            click.echo("   Command line: data-validate validate config.yaml -d \"|\"")
            click.echo("   YAML config:  delimiter: \"|\"  (under files section)")
        release_lock()
        clean_exit(1)

    except Exception as e:
        po.progress_done()
        po.blank_line()

        # Get exception type for context-aware error messages
        error_type = type(e).__name__
        error_msg = str(e)

        po.error(f"Unexpected error: {error_type}")
        if error_msg:
            click.echo(f"   {error_msg}", err=True)
        logger.error(f"Unexpected error ({error_type}): {error_msg}", exc_info=True)

        po.blank_line()

        # Context-aware help based on error type
        if "encoding" in error_msg.lower() or "codec" in error_msg.lower():
            po.info("This looks like an encoding issue. Try:")
            click.echo("   - Specify encoding in config: encoding: \"utf-8\" or \"cp1252\"")
            click.echo("   - Convert file to UTF-8 before validation")
        elif "delimiter" in error_msg.lower() or "parse" in error_msg.lower():
            po.info("This looks like a parsing issue. Try:")
            click.echo("   - Specify delimiter: data-validate validate config.yaml -d \"|\"")
            click.echo("   - Check for malformed rows (unquoted delimiters in data)")
        elif "memory" in error_msg.lower() or "allocate" in error_msg.lower():
            po.info("This looks like a memory issue. Try:")
            click.echo("   - Use smaller chunk size: chunk_size: 10000")
            click.echo("   - Process files individually")
        else:
            po.info("Troubleshooting:")
            click.echo("   - Check log file for details (use --log-file)")
            click.echo("   - Run with --log-level DEBUG for more info")
            click.echo("   - Report issue: https://github.com/danieledge/DataK9/issues")

        # Only show traceback in verbose mode
        if verbose:
            po.blank_line()
            po.warning("Traceback (verbose mode):")
            import traceback
            traceback.print_exc()
        else:
            po.blank_line()
            po.info("Run with -v flag to see full error traceback")

        release_lock()
        clean_exit(1)


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
@click.option('--delimiter', '-d', default=None, help='Column delimiter for CSV files (default: comma). Use "\\t" for tab-separated files.')
@click.option('--skip-rows', type=int, default=None, help='Number of rows to skip before the header row. Use when file has identifier/metadata lines before headers.')
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
@click.option('--no-ml', is_flag=True, help='Disable ML-based anomaly detection (Benford, outliers, autoencoder)')
@click.option('--full-analysis', is_flag=True, help='Disable internal sampling - analyze full dataset (slower but more accurate for ML analysis)')
@click.option('--analysis-sample-size', type=int, default=100000, help='Sample size for analysis when file exceeds this many rows (default: 100000). Files <= this size are analyzed fully.')
@click.option('--field-descriptions', type=click.Path(exists=True), help='YAML file with friendly field names and descriptions for better anomaly explanations')
@click.option('--correlation-threshold', type=float, default=None, help='Minimum absolute correlation to report (default: 0.3, Cohen\'s medium effect). Range: 0.0-1.0')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed progress with timestamps for debugging')
def profile(file_path, format, delimiter, skip_rows, database, table, query, html_output, json_output, config_output, chunk_size, sample, no_memory_check, log_level,
            disable_temporal, disable_pii, disable_correlation, disable_all_enhancements, no_ml, full_analysis, analysis_sample_size, field_descriptions, correlation_threshold, verbose):
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

        # Load field descriptions and profiler settings from context file if provided
        field_desc_dict = None
        context_config = {}
        if field_descriptions:
            try:
                from validation_framework.profiler.context_discovery import load_field_descriptions
                import yaml
                # Load full config to get profiler settings
                with open(field_descriptions, 'r') as f:
                    context_config = yaml.safe_load(f) or {}
                field_desc_dict = context_config.get('field_descriptions', {})
                if field_desc_dict:
                    po.info(f"Loaded {len(field_desc_dict)} field descriptions")
            except Exception as e:
                logger.warning(f"Could not load field descriptions: {e}")

        # Determine correlation threshold: CLI overrides context file, else use default
        # Priority: CLI > context file > default (0.3)
        effective_correlation_threshold = 0.3  # Default
        if 'profiler_settings' in context_config and 'correlation_threshold' in context_config['profiler_settings']:
            effective_correlation_threshold = float(context_config['profiler_settings']['correlation_threshold'])
            logger.debug(f"Using correlation_threshold from context file: {effective_correlation_threshold}")
        if correlation_threshold is not None:
            # CLI always overrides
            effective_correlation_threshold = correlation_threshold
            logger.debug(f"CLI override for correlation_threshold: {effective_correlation_threshold}")

        # Validate threshold range
        if not 0.0 <= effective_correlation_threshold <= 1.0:
            click.echo(f"⚠️ Warning: correlation_threshold {effective_correlation_threshold} outside valid range [0.0, 1.0], using 0.3", err=True)
            effective_correlation_threshold = 0.3

        # Initialize profiler with enhancements (enabled by default, disabled if flag set)
        profiler = DataProfiler(
            chunk_size=chunk_size,
            correlation_threshold=effective_correlation_threshold,
            enable_temporal_analysis=not disable_temporal,
            enable_pii_detection=not disable_pii,
            enable_enhanced_correlation=not disable_correlation,
            disable_memory_safety=no_memory_check,  # Pass through the --no-memory-check flag
            full_analysis=full_analysis,  # Disable internal sampling for ML analysis
            analysis_sample_size=analysis_sample_size,  # Configurable sample size
            field_descriptions=field_desc_dict  # For context-aware anomaly detection
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

            po.task_start(f"Profiling database: {table if table else 'query'}")

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
                po.task_start(f"Profiling {file_path} (first {sample:,} rows)")
            else:
                po.task_start(f"Profiling {file_path}")
            # Build loader kwargs
            loader_kwargs = {}
            if delimiter:
                # Handle escape sequences like \t for tab
                loader_kwargs['delimiter'] = delimiter.encode().decode('unicode_escape')
            elif format == 'csv':
                # Auto-detect delimiter for CSV files
                detected_delimiter = detect_csv_delimiter(file_path)
                if detected_delimiter and detected_delimiter != ',':
                    loader_kwargs['delimiter'] = detected_delimiter
                    delim_display = repr(detected_delimiter).strip("'")
                    po.info(f"Auto-detected delimiter: {delim_display}")

            # Handle skip rows (for files with metadata/identifier lines before headers)
            if skip_rows:
                loader_kwargs['skiprows'] = skip_rows
                po.info(f"Skipping first {skip_rows} row(s) before header")

            # Create progress reporter (handles both normal and verbose output)
            reporter = VerboseProgressReporter(verbose=verbose)

            # Verbose callback for column-level progress
            def verbose_callback(event_type, **kwargs):
                """Handle verbose events from profiler."""
                if event_type == 'column_start':
                    reporter.column_start(kwargs['col_idx'], kwargs['total_cols'], kwargs['col_name'])
                elif event_type == 'column_complete':
                    reporter.column_complete(
                        kwargs['col_idx'], kwargs['total_cols'], kwargs['col_name'],
                        elapsed=kwargs.get('elapsed')
                    )
                elif event_type == 'debug':
                    reporter.debug(kwargs.get('message', ''))

            profile_result = profiler.profile_file(
                file_path=file_path,
                file_format=format,
                sample_rows=sample,
                progress_callback=reporter.callback,
                verbose_callback=verbose_callback if verbose else None,
                **loader_kwargs
            )
            po.progress_done()  # Clear the progress line
            reporter.phase_complete()  # Show final phase timing
            reporter.summary()  # Show slow column summary if any

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

        # Visual profile summary
        po.profile_summary(
            rows=profile_result.row_count,
            cols=profile_result.column_count,
            quality=profile_result.overall_quality_score,
            duration=profile_result.processing_time_seconds,
            size_str=size_str
        )

        # Calculate type breakdown for structure summary
        type_breakdown = {}
        for col in profile_result.columns:
            inferred = col.type_info.inferred_type if col.type_info else "unknown"
            # Group types
            if inferred in ("int64", "float64", "integer", "float", "numeric"):
                type_breakdown["numeric"] = type_breakdown.get("numeric", 0) + 1
            elif inferred in ("string", "object", "str"):
                type_breakdown["string"] = type_breakdown.get("string", 0) + 1
            elif inferred in ("datetime64", "datetime", "date", "timestamp"):
                type_breakdown["datetime"] = type_breakdown.get("datetime", 0) + 1
            elif inferred in ("bool", "boolean"):
                type_breakdown["boolean"] = type_breakdown.get("boolean", 0) + 1
            else:
                type_breakdown["other"] = type_breakdown.get("other", 0) + 1

        # Structure summary
        po.structure_summary(profile_result.column_count, type_breakdown)

        # Quality summary
        quality_issues = sum(len(col.quality.issues) for col in profile_result.columns if col.quality)
        po.quality_summary(profile_result.overall_quality_score, quality_issues)

        # Correlations summary
        correlation_count = len(profile_result.correlations) if profile_result.correlations else 0
        strong_correlations = sum(1 for c in (profile_result.correlations or [])
                                  if abs(c.correlation) >= 0.7)
        po.correlations_summary(correlation_count, strong_correlations)

        # Temporal summary (count datetime fields with temporal analysis)
        temporal_count = sum(1 for col in profile_result.columns
                            if col.temporal_analysis and col.temporal_analysis.get("available"))
        po.temporal_summary(temporal_count)

        # Run ML analysis by default (unless --no-ml flag is set)
        # NOTE: The profiler engine already runs ML analysis during profiling (50K sample)
        # Only run additional analysis here if ml_findings is not already populated
        if not no_ml:
            if profile_result.ml_findings:
                # ML analysis already completed during profiling - display results
                ml_findings = profile_result.ml_findings
                summary = ml_findings.get("summary", {})
                total_issues = summary.get("total_issues", 0)
                severity = summary.get("severity", "none")
                key_findings = summary.get("key_findings", [])
                analyzed_rows = ml_findings.get('sample_info', {}).get('analyzed_rows',
                               ml_findings.get('sample_info', {}).get('sample_size', 0))

                po.ml_summary(total_issues, severity, key_findings, analyzed_rows)
            else:
                # No ML findings from profiler - run separate analysis
                po.task_start("Running ML-based anomaly detection", icon=po.BRAIN)
                try:
                    from validation_framework.profiler.ml_analyzer import run_ml_analysis

                    # Load sample data for ML analysis
                    ml_sample_size = 250_000
                    loader = LoaderFactory.create_loader(file_path, format) if file_path else None

                    if loader:
                        # Load sample for ML analysis
                        sample_df = None
                        rows_loaded = 0
                        chunks = []
                        for chunk in loader.load():
                            chunks.append(chunk)
                            rows_loaded += len(chunk)
                            if rows_loaded >= ml_sample_size:
                                break

                        if chunks:
                            import pandas as pd
                            sample_df = pd.concat(chunks, ignore_index=True)
                            if len(sample_df) > ml_sample_size:
                                sample_df = sample_df.head(ml_sample_size)

                            # Extract semantic info from profile_result for intelligent ML analysis
                            column_semantic_info = {}
                            for col_profile in profile_result.columns:
                                if col_profile.semantic_info:
                                    column_semantic_info[col_profile.name] = col_profile.semantic_info

                            # Run ML analysis with semantic context
                            ml_findings = run_ml_analysis(
                                sample_df,
                                column_semantic_info=column_semantic_info
                            )
                            profile_result.ml_findings = ml_findings

                            # Display summary using PrettyOutput
                            summary = ml_findings.get("summary", {})
                            total_issues = summary.get("total_issues", 0)
                            severity = summary.get("severity", "none")
                            key_findings = summary.get("key_findings", [])
                            analyzed_rows = ml_findings.get('sample_info', {}).get('analyzed_rows', 0)

                            po.ml_summary(total_issues, severity, key_findings, analyzed_rows)

                            # Clean up
                            del sample_df
                            import gc
                            gc.collect()

                except ImportError as e:
                    po.warning(f"ML analysis requires scikit-learn: {e}")
                except Exception as e:
                    po.warning(f"ML analysis failed: {e}")
                    logger.debug(f"ML analysis error: {e}", exc_info=True)

        # Suggested validations summary
        validation_count = len(profile_result.suggested_validations) if profile_result.suggested_validations else 0
        po.validations_summary(validation_count)

        # Generate HTML report
        reporter = ExecutiveHTMLReporter()
        reporter.generate_report(profile_result, html_output)

        # Output files section
        po.blank_line()
        po.subsection("Output Files")
        po.output_file("HTML Report", html_output)

        # Generate JSON output if requested
        if json_output:
            import json
            # Expand patterns in JSON output path
            context = {'file_name': Path(file_path).stem if file_path else (table or 'query_result')}
            json_output = expander.expand(json_output, context)

            with open(json_output, 'w') as f:
                json.dump(profile_result.to_dict(), f, indent=2)
            po.output_file("JSON Profile", json_output)

        # Save generated validation config
        if profile_result.generated_config_yaml:
            with open(config_output, 'w') as f:
                f.write(profile_result.generated_config_yaml)
            po.output_file(f"Suggested Config ({validation_count} rules)", config_output)

        po.blank_line()
        sys.exit(0)

    except KeyboardInterrupt:
        # User pressed Ctrl+C
        po.progress_done()  # Clear any progress line
        po.blank_line()
        po.warning("Profiling cancelled by user (Ctrl+C)")
        sys.exit(130)  # Standard exit code for Ctrl+C

    except MemoryError as e:
        # Memory safety termination from profiler or system OOM
        po.progress_done()  # Clear any progress line
        po.blank_line()
        po.error("Memory limit exceeded - profiler terminated to prevent system instability")
        error_msg = str(e)
        if error_msg:
            po.blank_line()
            click.echo(f"   {error_msg}", err=True)
        po.blank_line()
        po.info("Solutions:")
        click.echo("   1. Use --sample to profile a subset: --sample 100000")
        click.echo("   2. Process file in smaller chunks: --chunk-size 10000")
        click.echo("   3. Close other applications to free memory")
        click.echo("   4. Convert large CSV files to Parquet format (more efficient)")
        if not no_memory_check:
            click.echo("   5. Use --no-memory-check to disable safety limits (USE WITH CAUTION)")
        sys.exit(137)  # Standard exit code for OOM-killed processes

    except FileNotFoundError as e:
        po.blank_line()
        po.error(f"File not found: {str(e)}")
        sys.exit(1)

    except RuntimeError as e:
        # Graceful error from loaders (CSV parsing, encoding issues)
        error_msg = str(e)
        po.blank_line()
        po.error("Error processing file:")
        click.echo(f"   {error_msg}", err=True)
        if "delimiter" in error_msg.lower():
            po.blank_line()
            po.info("Tip: Try specifying the delimiter with -d option:")
            click.echo(f"   python -m validation_framework.cli profile {file_path} -d \"|\"")
            click.echo(f"   python -m validation_framework.cli profile {file_path} -d \"\\t\"  # for tabs")
        sys.exit(1)

    except Exception as e:
        po.progress_done()  # Clear any progress line
        po.blank_line()

        # Get exception type for context-aware error messages
        error_type = type(e).__name__
        error_msg = str(e)

        po.error(f"Unexpected error: {error_type}")
        if error_msg:
            click.echo(f"   {error_msg}", err=True)

        po.blank_line()

        # Context-aware help based on error type
        if "encoding" in error_msg.lower() or "codec" in error_msg.lower():
            po.info("This looks like an encoding issue. Try:")
            click.echo("   - Specify encoding: check file encoding (UTF-8, CP1252, Latin-1)")
            click.echo("   - Convert file to UTF-8 before profiling")
        elif "delimiter" in error_msg.lower() or "parse" in error_msg.lower():
            po.info("This looks like a parsing issue. Try:")
            click.echo(f"   - Specify delimiter: data-validate profile {file_path} -d \"|\"")
            click.echo("   - Check for malformed rows (unquoted delimiters in data)")
        elif "memory" in error_msg.lower() or "allocate" in error_msg.lower():
            po.info("This looks like a memory issue. Try:")
            click.echo("   - Use --sample to profile a subset: --sample 100000")
            click.echo("   - Use smaller chunks: --chunk-size 10000")
        else:
            po.info("Troubleshooting:")
            click.echo("   - Run with -v flag for more details")
            click.echo("   - Check file format and integrity")
            click.echo("   - Report issue: https://github.com/danieledge/DataK9/issues")

        # Only show traceback in verbose mode
        if verbose:
            po.blank_line()
            po.warning("Traceback (verbose mode):")
            import traceback
            traceback.print_exc()
        else:
            po.blank_line()
            po.info("Run with -v flag to see full error traceback")

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


@cli.command('check-policy')
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--policy', '-p', type=click.Choice(['none', 'minimal', 'standard', 'strict']),
              default=None, help='Policy to check against (overrides config)')
@click.option('--fix', is_flag=True, help='Generate fixed config with missing checks added')
@click.option('--output', '-o', help='Output path for fixed config (default: {config}_fixed.yaml)')
@click.option('--json-output', '-j', help='Path for JSON policy report')
def check_policy(config_file, policy, fix, output, json_output):
    """
    Check configuration against validation policy requirements.

    This command analyzes your validation configuration to detect missing
    required checks according to the specified policy.

    CONFIG_FILE: Path to YAML configuration file to check

    Available policies:
    - none: No policy enforcement (all checks optional)
    - minimal: Basic checks only (EmptyFileCheck)
    - standard: Recommended for most projects (default)
    - strict: Comprehensive checks for critical pipelines

    Examples:

    \b
    # Check against standard policy
    python3 -m validation_framework.cli check-policy config.yaml

    \b
    # Check against strict policy
    python3 -m validation_framework.cli check-policy config.yaml --policy strict

    \b
    # Generate fixed configuration with missing checks
    python3 -m validation_framework.cli check-policy config.yaml --fix

    \b
    # Output JSON report for CI/CD
    python3 -m validation_framework.cli check-policy config.yaml -j policy_report.json
    """
    import yaml
    import json as json_module
    from validation_framework.policy import (
        PolicyAnalyzer, POLICIES, get_policy,
        EnforcementMode, get_default_check_config
    )

    po.logo()
    po.header("Policy Compliance Check")

    try:
        # Load configuration
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        job_config = config.get('validation_job', config)

        # Determine which policy to use
        if policy:
            # CLI override
            policy_obj = get_policy(policy)
            po.info(f"Using CLI-specified policy: {policy}")
        elif 'policy' in job_config:
            # From config
            policy_name = job_config['policy']
            try:
                policy_obj = get_policy(policy_name)
            except KeyError:
                policy_obj = POLICIES['standard']
            po.info(f"Using config-specified policy: {policy_name}")
        else:
            # Default
            policy_obj = POLICIES['standard']
            po.info(f"Using default policy: standard")

        po.blank_line()

        # Display policy details
        po.subsection(f"Policy: {policy_obj.name}")
        po.info(f"Description: {policy_obj.description}")
        po.info(f"Enforcement: {policy_obj.enforcement.value}")

        if policy_obj.universal_checks:
            po.info(f"Universal checks: {', '.join(policy_obj.universal_checks)}")
        if policy_obj.format_checks:
            for fmt, checks in policy_obj.format_checks.items():
                po.info(f"{fmt.upper()} checks: {', '.join(checks)}")
        if policy_obj.cda_require_one_of:
            po.info(f"CDA require one of: {', '.join(policy_obj.cda_require_one_of)}")
        if policy_obj.cda_require_all:
            po.info(f"CDA require all: {', '.join(policy_obj.cda_require_all)}")

        po.blank_line()

        # Analyze configuration
        analyzer = PolicyAnalyzer(policy_obj)
        violations = analyzer.analyze(config)

        # Separate by severity
        required = [v for v in violations if v.severity == "required"]
        recommended = [v for v in violations if v.severity == "recommended"]

        # Display results
        if not violations:
            po.success(f"Configuration complies with '{policy_obj.name}' policy")
            po.blank_line()
            sys.exit(0)

        # Show violations
        po.subsection("Policy Violations")

        if required:
            po.error(f"Required checks missing ({len(required)}):")
            for v in required:
                fixable = " [auto-fixable]" if v.auto_fixable else " [needs params]"
                po.warning(f"  {v.file_name}: {v.check_type}{fixable}")
                po.info(f"    Reason: {v.reason}")

        if recommended:
            po.blank_line()
            po.warning(f"Recommended checks missing ({len(recommended)}):")
            for v in recommended:
                po.info(f"  {v.file_name}: {v.check_type}")

        po.blank_line()

        # Summary
        summary_items = [
            ("Required Missing", str(len(required)), "red" if required else "green"),
            ("Recommended Missing", str(len(recommended)), "yellow" if recommended else "green"),
            ("Auto-Fixable", str(sum(1 for v in violations if v.auto_fixable)), "cyan"),
        ]
        po.summary_box("Policy Check Summary", summary_items)

        # Generate fixed config if requested
        if fix:
            po.blank_line()
            po.subsection("Generating Fixed Configuration")

            # Only fix auto-fixable violations
            fixable = [v for v in required if v.auto_fixable]
            if not fixable:
                po.warning("No auto-fixable violations found")
                po.info("Some violations require manual configuration (field names, patterns, etc.)")
            else:
                # Deep copy config and add missing checks
                import copy
                fixed_config = copy.deepcopy(config)
                fixed_job = fixed_config.get('validation_job', fixed_config)

                for violation in fixable:
                    # Find file config
                    for file_config in fixed_job.get('files', []):
                        if file_config.get('name') == violation.file_name:
                            default_check = get_default_check_config(violation.check_type)
                            # Convert Severity enum to string for YAML
                            if 'severity' in default_check:
                                default_check['severity'] = default_check['severity'].value if hasattr(default_check['severity'], 'value') else str(default_check['severity'])
                            if 'validations' not in file_config:
                                file_config['validations'] = []
                            file_config['validations'].insert(0, default_check)
                            po.success(f"Added {violation.check_type} to {violation.file_name}")
                            break

                # Determine output path
                if not output:
                    output = config_file.replace('.yaml', '_fixed.yaml').replace('.yml', '_fixed.yml')
                    if output == config_file:
                        output = config_file + '_fixed'

                with open(output, 'w') as f:
                    yaml.dump(fixed_config, f, default_flow_style=False, sort_keys=False)
                po.success(f"Fixed configuration saved to: {output}")

        # Generate JSON report if requested
        if json_output:
            summary = analyzer.get_summary(violations)
            json_data = {
                'config_file': config_file,
                'policy': policy_obj.to_dict(),
                'compliant': len(required) == 0,
                'summary': summary,
                'violations': [
                    {
                        'file_name': v.file_name,
                        'check_type': v.check_type,
                        'reason': v.reason,
                        'severity': v.severity,
                        'auto_fixable': v.auto_fixable,
                        'cda_field': v.cda_field,
                    }
                    for v in violations
                ]
            }
            with open(json_output, 'w') as f:
                json_module.dump(json_data, f, indent=2)
            po.success(f"JSON report saved to: {json_output}")

        # Exit code based on required violations
        if required:
            po.blank_line()
            po.error(f"Policy check failed: {len(required)} required check(s) missing")
            sys.exit(1)
        else:
            po.blank_line()
            po.warning("Policy check passed with recommendations")
            sys.exit(0)

    except FileNotFoundError as e:
        po.error(f"File not found: {str(e)}")
        sys.exit(1)

    except Exception as e:
        po.error(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


@cli.command('list-policies')
def list_policies():
    """
    List all available validation policies.

    Shows built-in policies and their requirements.

    Examples:

    \b
    python3 -m validation_framework.cli list-policies
    """
    from validation_framework.policy import POLICIES

    po.logo()
    po.header("Available Validation Policies")
    po.blank_line()

    for name, policy in POLICIES.items():
        po.subsection(f"{name.upper()}")
        po.info(f"Description: {policy.description}")
        po.info(f"Enforcement: {policy.enforcement.value}")

        if policy.universal_checks:
            po.info(f"Universal: {', '.join(policy.universal_checks)}")
        else:
            po.info("Universal: (none)")

        if policy.format_checks:
            for fmt, checks in policy.format_checks.items():
                po.info(f"{fmt.upper()}: {', '.join(checks)}")

        if policy.cda_require_all:
            po.info(f"CDA require all: {', '.join(policy.cda_require_all)}")

        if policy.cda_require_one_of:
            po.info(f"CDA require one of: {', '.join(policy.cda_require_one_of)}")

        if policy.recommended_checks:
            po.info(f"Recommended: {', '.join(policy.recommended_checks)}")

        po.blank_line()

    po.info("Use --policy flag with validate or check-policy commands to select a policy")
    po.blank_line()


if __name__ == '__main__':
    cli()
