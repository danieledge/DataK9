// Auto-generated from validation_definitions.json
// Total validations: 35
// Generated: 2025-11-15

const validationLibrary = {
    'AdvancedAnomalyDetectionCheck': {
        icon: '🤖',
        name: 'AdvancedAnomalyDetection Check',
        type: 'AdvancedAnomalyDetectionCheck',
        category: 'Statistical',
        description: 'ML-based anomaly detection using isolation forest or local outlier factor',
        params: [
            {
                name: 'fields',
                label: 'Fields (comma-separated)',
                type: 'text',
                required: true,
                help: 'Numeric fields to analyze for anomalies'
            },
            {
                name: 'method',
                label: 'Detection Method',
                type: 'select',
                required: false,
                help: 'Machine learning algorithm',
                options: ['isolation_forest', 'local_outlier_factor', 'one_class_svm'],
                default: 'isolation_forest'
            },
            {
                name: 'contamination',
                label: 'Expected Outlier Rate',
                type: 'number',
                required: false,
                help: 'Expected proportion of outliers (0.0-0.5)',
                min: 0,
                max: 0.5,
                default: 0.1
            }
        ],
        examples: 'Detect fraudulent transactions using isolation forest',
        tips: 'Advanced ML-based detection for complex patterns'
    },
    'BaselineComparisonCheck': {
        icon: '📅',
        name: 'BaselineComparison Check',
        type: 'BaselineComparisonCheck',
        category: 'Temporal',
        description: 'Compares current data against historical baseline',
        params: [
            {
                name: 'metric',
                label: 'Metric',
                type: 'select',
                required: true,
                help: 'What to compare',
                options: ['row_count', 'sum', 'average', 'min', 'max']
            },
            {
                name: 'field',
                label: 'Field',
                type: 'text',
                required: false,
                help: 'Field to measure (not needed for row_count)'
            },
            {
                name: 'baseline_file',
                label: 'Baseline File',
                type: 'text',
                required: true,
                help: 'Historical baseline file path'
            },
            {
                name: 'tolerance_percent',
                label: 'Tolerance (%)',
                type: 'number',
                required: true,
                help: 'Acceptable variance from baseline',
                min: 0,
                default: 10
            }
        ],
        examples: 'Compare to yesterday\'s row count: baseline_file: previous_day.csv, tolerance: 10',
        tips: 'Detect unusual data volumes or metric changes'
    },
    'BlankRecordCheck': {
        icon: '⚠️',
        name: 'BlankRecord Check',
        type: 'BlankRecordCheck',
        category: 'Record-Level',
        description: 'Checks for completely blank rows (all fields empty/null)',
        params: [],
        examples: 'Detect and flag empty rows in data files',
        tips: 'Useful for catching malformed CSV exports with trailing blank rows'
    },
    'ColumnPresenceCheck': {
        icon: '📋',
        name: 'ColumnPresence Check',
        type: 'ColumnPresenceCheck',
        category: 'Schema',
        description: 'Checks that required columns exist in the file',
        params: [
            {
                name: 'required_columns',
                label: 'Required Columns (comma-separated)',
                type: 'text',
                required: true,
                help: 'Columns that must be present',
                placeholder: 'customer_id,order_date,amount'
            },
            {
                name: 'case_sensitive',
                label: 'Case Sensitive',
                type: 'checkbox',
                required: false,
                help: 'Whether column name matching is case-sensitive',
                default: true
            }
        ],
        examples: 'Ensure critical columns exist: required_columns: [customer_id, order_date]',
        tips: 'Lighter weight than SchemaMatchCheck when you only care about presence'
    },
    'CompletenessCheck': {
        icon: '💯',
        name: 'Completeness Check',
        type: 'CompletenessCheck',
        category: 'Advanced',
        description: 'Validates field completeness (non-null percentage) meets threshold',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Field to check completeness'
            },
            {
                name: 'min_completeness',
                label: 'Minimum Completeness (%)',
                type: 'number',
                required: true,
                help: 'Minimum acceptable percentage of non-null values (0-100)',
                min: 0,
                max: 100,
                default: 95
            }
        ],
        examples: 'Email at least 95% complete: field: email, min_completeness: 95',
        tips: 'Use WARNING for optional fields, ERROR for critical fields'
    },
    'ConditionalValidation': {
        icon: '🔀',
        name: 'Conditional Validation',
        type: 'ConditionalValidation',
        category: 'Conditional',
        description: 'Executes validation rules conditionally based on SQL-like expression',
        params: [
            {
                name: 'condition',
                label: 'Condition (SQL-like)',
                type: 'text',
                required: true,
                help: 'SQL-like WHERE clause to evaluate',
                placeholder: 'status = \'Active\' AND age > 18'
            },
            {
                name: 'validations',
                label: 'Validations (JSON)',
                type: 'json',
                required: true,
                help: 'List of validations to run when condition is true (as JSON array)'
            }
        ],
        examples: 'IF status=\'Active\' THEN validate email field',
        tips: 'Powerful for complex business rules with conditional logic'
    },
    'CorrelationCheck': {
        icon: '🔗',
        name: 'Correlation Check',
        type: 'CorrelationCheck',
        category: 'Statistical',
        description: 'Validates correlation between two numeric fields',
        params: [
            {
                name: 'field_a',
                label: 'Field A',
                type: 'text',
                required: true,
                help: 'First numeric field'
            },
            {
                name: 'field_b',
                label: 'Field B',
                type: 'text',
                required: true,
                help: 'Second numeric field'
            },
            {
                name: 'min_correlation',
                label: 'Minimum Correlation',
                type: 'number',
                required: false,
                help: 'Minimum acceptable correlation coefficient',
                min: -1,
                max: 1
            },
            {
                name: 'max_correlation',
                label: 'Maximum Correlation',
                type: 'number',
                required: false,
                help: 'Maximum acceptable correlation coefficient',
                min: -1,
                max: 1
            },
            {
                name: 'method',
                label: 'Correlation Method',
                type: 'select',
                required: false,
                help: 'Statistical correlation method',
                options: ['pearson', 'spearman', 'kendall'],
                default: 'pearson'
            }
        ],
        examples: 'Validate price and quantity are negatively correlated',
        tips: 'Detect unexpected relationships between variables'
    },
    'CrossFieldComparisonCheck': {
        icon: '⚡',
        name: 'CrossFieldComparison Check',
        type: 'CrossFieldComparisonCheck',
        category: 'Advanced',
        description: 'Validates relationship between two fields (e.g., start < end)',
        params: [
            {
                name: 'field_a',
                label: 'Field A',
                type: 'text',
                required: true,
                help: 'First field in comparison'
            },
            {
                name: 'operator',
                label: 'Operator',
                type: 'select',
                required: true,
                help: 'Comparison operator',
                options: ['<', '<=', '>', '>=', '==', '!=']
            },
            {
                name: 'field_b',
                label: 'Field B',
                type: 'text',
                required: true,
                help: 'Second field in comparison'
            }
        ],
        examples: 'Start before end: field_a: start_date, operator: <, field_b: end_date',
        tips: 'Useful for date ranges, min/max validation, logical consistency'
    },
    'CrossFileComparisonCheck': {
        icon: '📊',
        name: 'CrossFileComparison Check',
        type: 'CrossFileComparisonCheck',
        category: 'Cross-File',
        description: 'Compares metrics between two files (row counts, sums, etc.)',
        params: [
            {
                name: 'metric',
                label: 'Metric',
                type: 'select',
                required: true,
                help: 'What to compare',
                options: ['row_count', 'sum', 'average', 'min', 'max']
            },
            {
                name: 'field',
                label: 'Field',
                type: 'text',
                required: false,
                help: 'Field to aggregate (not needed for row_count)'
            },
            {
                name: 'reference_file',
                label: 'Reference File',
                type: 'text',
                required: true,
                help: 'File to compare against'
            },
            {
                name: 'reference_field',
                label: 'Reference Field',
                type: 'text',
                required: false,
                help: 'Field in reference file (if different)'
            },
            {
                name: 'tolerance_percent',
                label: 'Tolerance (%)',
                type: 'number',
                required: false,
                help: 'Acceptable variance percentage',
                min: 0,
                default: 0
            }
        ],
        examples: 'Compare row counts: metric: row_count, reference_file: previous_load.csv',
        tips: 'Useful for data reconciliation and consistency checks'
    },
    'CrossFileDuplicateCheck': {
        icon: '🔍',
        name: 'CrossFileDuplicate Check',
        type: 'CrossFileDuplicateCheck',
        category: 'Cross-File',
        description: 'Checks for duplicate keys across multiple files',
        params: [
            {
                name: 'key_fields',
                label: 'Key Fields (comma-separated)',
                type: 'text',
                required: true,
                help: 'Fields that define uniqueness'
            },
            {
                name: 'reference_files',
                label: 'Reference Files (comma-separated)',
                type: 'text',
                required: true,
                help: 'Files to check for duplicates'
            }
        ],
        examples: 'Check transaction_id unique across files: key_fields: [transaction_id]',
        tips: 'Prevents duplicate records across incremental loads'
    },
    'CrossFileKeyCheck': {
        icon: '🔗',
        name: 'CrossFileKey Check',
        type: 'CrossFileKeyCheck',
        category: 'Cross-File',
        description: 'Advanced cross-file referential integrity check with multiple check modes and memory-efficient processing',
        params: [
            {
                name: 'foreign_key',
                label: 'Foreign Key Column(s)',
                type: 'text',
                required: true,
                help: 'Column(s) to validate (comma-separated for composite keys)',
                placeholder: 'customer_id'
            },
            {
                name: 'reference_file',
                label: 'Reference File Path',
                type: 'text',
                required: true,
                help: 'Path to reference file containing valid keys',
                placeholder: 'data/customers.csv'
            },
            {
                name: 'reference_key',
                label: 'Reference Key Column(s)',
                type: 'text',
                required: true,
                help: 'Column(s) in reference file to check against',
                placeholder: 'id'
            },
            {
                name: 'check_mode',
                label: 'Check Mode',
                type: 'select',
                required: false,
                default: 'exact_match',
                help: 'Type of referential integrity check to perform',
                options: ['exact_match', 'overlap', 'subset', 'superset']
            },
            {
                name: 'allow_null',
                label: 'Allow Null Values',
                type: 'boolean',
                required: false,
                default: false,
                help: 'Whether to allow NULL foreign key values'
            },
            {
                name: 'min_overlap_pct',
                label: 'Minimum Overlap %',
                type: 'number',
                required: false,
                default: 1.0,
                min: 0,
                max: 100,
                help: 'Minimum percentage of keys that must overlap (for overlap mode)'
            },
            {
                name: 'reference_file_format',
                label: 'Reference File Format',
                type: 'select',
                required: false,
                default: 'csv',
                help: 'Format of reference file',
                options: ['csv', 'parquet', 'json', 'excel']
            }
        ],
        examples: 'Validate customer_id exists in customers.csv: foreign_key: customer_id, reference_file: customers.csv, reference_key: id, check_mode: exact_match',
        tips: 'Memory-efficient validation supporting billions of keys with automatic disk spillover. Use exact_match for strict FK checks, overlap for partial matching, subset for validation, superset for completeness checks.'
    },
    'DatabaseConstraintCheck': {
        icon: '🗄️',
        name: 'DatabaseConstraint Check',
        type: 'DatabaseConstraintCheck',
        category: 'Database',
        description: 'Validates data against database constraints (PK, FK, unique, not null)',
        params: [
            {
                name: 'connection_string',
                label: 'Database Connection String',
                type: 'text',
                required: true,
                help: 'Database connection string'
            },
            {
                name: 'table_name',
                label: 'Table Name',
                type: 'text',
                required: true,
                help: 'Target database table'
            },
            {
                name: 'constraints',
                label: 'Constraints to Check',
                type: 'select',
                required: true,
                help: 'Which constraints to validate',
                options: ['all', 'primary_key', 'foreign_key', 'unique', 'not_null']
            }
        ],
        examples: 'Validate all constraints before insert: table_name: customers, constraints: all',
        tips: 'Catch constraint violations before attempting database load'
    },
    'DatabaseReferentialIntegrityCheck': {
        icon: '🔗',
        name: 'DatabaseReferentialIntegrity Check',
        type: 'DatabaseReferentialIntegrityCheck',
        category: 'Database',
        description: 'Validates foreign key relationships against actual database',
        params: [
            {
                name: 'connection_string',
                label: 'Database Connection String',
                type: 'text',
                required: true,
                help: 'Database connection string'
            },
            {
                name: 'local_field',
                label: 'Local Field',
                type: 'text',
                required: true,
                help: 'Foreign key field in data'
            },
            {
                name: 'reference_table',
                label: 'Reference Table',
                type: 'text',
                required: true,
                help: 'Database table containing valid values'
            },
            {
                name: 'reference_column',
                label: 'Reference Column',
                type: 'text',
                required: true,
                help: 'Column in reference table (primary key)'
            }
        ],
        examples: 'Validate customer_id exists in database: reference_table: customers',
        tips: 'Real-time validation against live database'
    },
    'DateFormatCheck': {
        icon: '📅',
        name: 'DateFormat Check',
        type: 'DateFormatCheck',
        category: 'Field-Level',
        description: 'Validates date field matches expected format',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Date field to validate'
            },
            {
                name: 'format',
                label: 'Date Format',
                type: 'text',
                required: true,
                help: 'Expected date format (Python strftime)',
                placeholder: '%Y-%m-%d',
                default: '%Y-%m-%d'
            }
        ],
        examples: 'ISO format: format: \'%Y-%m-%d\' or US format: \'%m/%d/%Y\'',
        tips: 'Common formats: %Y-%m-%d (ISO), %m/%d/%Y (US), %d/%m/%Y (EU)'
    },
    'DistributionCheck': {
        icon: '📊',
        name: 'Distribution Check',
        type: 'DistributionCheck',
        category: 'Statistical',
        description: 'Validates data distribution matches expected pattern',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Field to analyze distribution'
            },
            {
                name: 'distribution_type',
                label: 'Expected Distribution',
                type: 'select',
                required: true,
                help: 'Expected statistical distribution',
                options: ['normal', 'uniform', 'exponential', 'custom']
            },
            {
                name: 'significance_level',
                label: 'Significance Level',
                type: 'number',
                required: false,
                help: 'P-value threshold for statistical test',
                min: 0,
                max: 1,
                default: 0.05
            }
        ],
        examples: 'Validate ages follow normal distribution',
        tips: 'Advanced statistical validation for data scientists'
    },
    'DuplicateRowCheck': {
        icon: '🔁',
        name: 'DuplicateRow Check',
        type: 'DuplicateRowCheck',
        category: 'Record-Level',
        description: 'Checks for duplicate rows based on key fields',
        params: [
            {
                name: 'key_fields',
                label: 'Key Fields (comma-separated)',
                type: 'text',
                required: true,
                help: 'Fields that define uniqueness',
                placeholder: 'customer_id,order_id'
            }
        ],
        examples: 'Check unique orders: key_fields: [customer_id, order_id]',
        tips: 'Use for primary key validation or business key uniqueness'
    },
    'EmptyFileCheck': {
        icon: '📄',
        name: 'EmptyFile Check',
        type: 'EmptyFileCheck',
        category: 'File-Level',
        description: 'Validates that the file is not empty and optionally contains data rows',
        params: [
            {
                name: 'check_data_rows',
                label: 'Check Data Rows',
                type: 'checkbox',
                required: false,
                help: 'If true, checks for actual data rows (not just headers)',
                default: true
            }
        ],
        examples: 'Ensure input files are not empty before processing',
        tips: 'Use ERROR severity to prevent processing empty files'
    },
    'FileSizeCheck': {
        icon: '💾',
        name: 'FileSize Check',
        type: 'FileSizeCheck',
        category: 'File-Level',
        description: 'Validates file size is within acceptable limits',
        params: [
            {
                name: 'min_size_mb',
                label: 'Min Size (MB)',
                type: 'number',
                required: false,
                help: 'Minimum file size in megabytes',
                min: 0
            },
            {
                name: 'max_size_mb',
                label: 'Max Size (MB)',
                type: 'number',
                required: false,
                help: 'Maximum file size in megabytes',
                min: 0
            },
            {
                name: 'max_size_gb',
                label: 'Max Size (GB)',
                type: 'number',
                required: false,
                help: 'Maximum file size in gigabytes',
                min: 0
            }
        ],
        examples: 'Prevent processing of unexpectedly large files',
        tips: 'Use to catch corrupt or malformed files early'
    },
    'FreshnessCheck': {
        icon: '🕐',
        name: 'Freshness Check',
        type: 'FreshnessCheck',
        category: 'Advanced',
        description: 'Validates data freshness based on timestamp field',
        params: [
            {
                name: 'timestamp_field',
                label: 'Timestamp Field',
                type: 'text',
                required: true,
                help: 'Field containing timestamp/date'
            },
            {
                name: 'max_age_hours',
                label: 'Maximum Age (hours)',
                type: 'number',
                required: true,
                help: 'Maximum acceptable age in hours',
                min: 0,
                default: 24
            }
        ],
        examples: 'Data within 24 hours: timestamp_field: created_at, max_age_hours: 24',
        tips: 'Useful for real-time or near-real-time data pipelines'
    },
    'InlineBusinessRuleCheck': {
        icon: '💼',
        name: 'InlineBusinessRule Check',
        type: 'InlineBusinessRuleCheck',
        category: 'Advanced',
        description: 'Custom business rule expressed as Python-like expression',
        params: [
            {
                name: 'expression',
                label: 'Business Rule Expression',
                type: 'textarea',
                required: true,
                help: 'Python-like expression to evaluate',
                placeholder: '(quantity * unit_price) == total_amount'
            },
            {
                name: 'message',
                label: 'Error Message',
                type: 'text',
                required: false,
                help: 'Custom error message when rule fails'
            }
        ],
        examples: 'Validate calculated fields: (quantity * unit_price) == total_amount',
        tips: 'Flexible business logic validation without writing code'
    },
    'InlineLookupCheck': {
        icon: '🔍',
        name: 'InlineLookup Check',
        type: 'InlineLookupCheck',
        category: 'Advanced',
        description: 'Validates field values against inline lookup list',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Field to validate'
            },
            {
                name: 'lookup_values',
                label: 'Lookup Values (JSON array)',
                type: 'json',
                required: true,
                help: 'Valid values as JSON array',
                placeholder: '["value1", "value2", "value3"]'
            }
        ],
        examples: 'Validate against inline list: lookup_values: ["US", "UK", "CA"]',
        tips: 'Quick alternative to ValidValuesCheck with JSON syntax'
    },
    'InlineRegexCheck': {
        icon: '🔤',
        name: 'InlineRegex Check',
        type: 'InlineRegexCheck',
        category: 'Advanced',
        description: 'Inline regex validation with custom pattern and field',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Field to validate'
            },
            {
                name: 'pattern',
                label: 'Regex Pattern',
                type: 'text',
                required: true,
                help: 'Regular expression pattern'
            }
        ],
        examples: 'Quick regex check without full RegexCheck configuration',
        tips: 'Lightweight alternative to RegexCheck for simple patterns'
    },
    'MandatoryFieldCheck': {
        icon: '📝',
        name: 'MandatoryField Check',
        type: 'MandatoryFieldCheck',
        category: 'Field-Level',
        description: 'Validates that specified fields are not null or empty',
        params: [
            {
                name: 'fields',
                label: 'Fields (comma-separated)',
                type: 'text',
                required: true,
                help: 'Fields that cannot be null or empty',
                placeholder: 'customer_id,order_id'
            }
        ],
        examples: 'Ensure primary keys are populated: fields: [customer_id, order_id]',
        tips: 'Use for critical fields that must always have values'
    },
    'NumericPrecisionCheck': {
        icon: '🔢',
        name: 'NumericPrecision Check',
        type: 'NumericPrecisionCheck',
        category: 'Advanced',
        description: 'Validates numeric precision (decimal places)',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Numeric field to validate'
            },
            {
                name: 'max_decimals',
                label: 'Maximum Decimal Places',
                type: 'number',
                required: true,
                help: 'Maximum allowed decimal places',
                min: 0,
                default: 2
            }
        ],
        examples: 'Currency with 2 decimals: field: amount, max_decimals: 2',
        tips: 'Ensures numeric precision matches database column definitions'
    },
    'RangeCheck': {
        icon: '📏',
        name: 'Range Check',
        type: 'RangeCheck',
        category: 'Field-Level',
        description: 'Validates numeric field values are within specified range',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Numeric field to validate'
            },
            {
                name: 'min_value',
                label: 'Minimum Value',
                type: 'number',
                required: false,
                help: 'Minimum acceptable value'
            },
            {
                name: 'max_value',
                label: 'Maximum Value',
                type: 'number',
                required: false,
                help: 'Maximum acceptable value'
            }
        ],
        examples: 'Age: min_value: 18, max_value: 120',
        tips: 'Use for numeric bounds validation (age, quantity, price, etc.)'
    },
    'ReferentialIntegrityCheck': {
        icon: '🔗',
        name: 'ReferentialIntegrity Check',
        type: 'ReferentialIntegrityCheck',
        category: 'Cross-File',
        description: 'Validates foreign key relationships between files',
        params: [
            {
                name: 'local_field',
                label: 'Local Field',
                type: 'text',
                required: true,
                help: 'Field in current file (foreign key)'
            },
            {
                name: 'reference_file',
                label: 'Reference File',
                type: 'text',
                required: true,
                help: 'File containing reference data'
            },
            {
                name: 'reference_field',
                label: 'Reference Field',
                type: 'text',
                required: true,
                help: 'Field in reference file (primary key)'
            }
        ],
        examples: 'Validate customer_id exists in customers file',
        tips: 'Ensures relational integrity across data files'
    },
    'RegexCheck': {
        icon: '🔤',
        name: 'Regex Check',
        type: 'RegexCheck',
        category: 'Field-Level',
        description: 'Validates field values match a regular expression pattern',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Field to validate'
            },
            {
                name: 'pattern',
                label: 'Regex Pattern',
                type: 'text',
                required: true,
                help: 'Regular expression pattern to match',
                placeholder: '^[A-Z]{2}\d{4}$'
            },
            {
                name: 'message',
                label: 'Error Message',
                type: 'text',
                required: false,
                help: 'Custom error message for failures'
            },
            {
                name: 'invert',
                label: 'Invert Match',
                type: 'checkbox',
                required: false,
                help: 'Fail if pattern DOES match (instead of does not match)',
                default: false
            }
        ],
        examples: 'Email: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        tips: 'Test patterns thoroughly - use online regex testers'
    },
    'RowCountRangeCheck': {
        icon: '📊',
        name: 'RowCountRange Check',
        type: 'RowCountRangeCheck',
        category: 'File-Level',
        description: 'Validates that the number of rows falls within specified bounds',
        params: [
            {
                name: 'min_rows',
                label: 'Minimum Rows',
                type: 'number',
                required: false,
                help: 'Minimum acceptable number of rows',
                min: 0
            },
            {
                name: 'max_rows',
                label: 'Maximum Rows',
                type: 'number',
                required: false,
                help: 'Maximum acceptable number of rows',
                min: 1
            }
        ],
        examples: 'Validate daily extract contains expected volume (e.g., min_rows: 1000)',
        tips: 'Set realistic bounds based on historical data patterns'
    },
    'SQLCustomCheck': {
        icon: '💾',
        name: 'SQLCustom Check',
        type: 'SQLCustomCheck',
        category: 'Database',
        description: 'Executes custom SQL query for validation',
        params: [
            {
                name: 'connection_string',
                label: 'Database Connection String',
                type: 'text',
                required: true,
                help: 'Database connection string'
            },
            {
                name: 'query',
                label: 'SQL Query',
                type: 'textarea',
                required: true,
                help: 'Custom SQL query to execute (must return boolean or row count)',
                placeholder: 'SELECT COUNT(*) FROM table WHERE condition'
            },
            {
                name: 'expected_result',
                label: 'Expected Result',
                type: 'text',
                required: false,
                help: 'Expected query result for validation'
            }
        ],
        examples: 'Custom business rule validation via SQL',
        tips: 'Maximum flexibility for complex database-driven validations'
    },
    'SchemaMatchCheck': {
        icon: '🎨',
        name: 'SchemaMatch Check',
        type: 'SchemaMatchCheck',
        category: 'Schema',
        description: 'Validates columns and their data types match expected schema',
        params: [
            {
                name: 'expected_columns',
                label: 'Expected Columns (comma-separated)',
                type: 'text',
                required: true,
                help: 'List of expected column names',
                placeholder: 'id,name,email,created_date'
            },
            {
                name: 'allow_extra',
                label: 'Allow Extra Columns',
                type: 'checkbox',
                required: false,
                help: 'Permit additional columns not in expected list',
                default: false
            },
            {
                name: 'allow_missing',
                label: 'Allow Missing Columns',
                type: 'checkbox',
                required: false,
                help: 'Permit missing columns from expected list',
                default: false
            }
        ],
        examples: 'Enforce strict schema: expected_columns: [id, name, email]',
        tips: 'Use strict matching (both false) for critical interfaces'
    },
    'StatisticalOutlierCheck': {
        icon: '📊',
        name: 'StatisticalOutlier Check',
        type: 'StatisticalOutlierCheck',
        category: 'Advanced',
        description: 'Detects statistical outliers using IQR or Z-score method',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Numeric field to check for outliers'
            },
            {
                name: 'method',
                label: 'Detection Method',
                type: 'select',
                required: false,
                help: 'Statistical method for outlier detection',
                options: ['iqr', 'zscore'],
                default: 'iqr'
            },
            {
                name: 'threshold',
                label: 'Threshold',
                type: 'number',
                required: false,
                help: 'IQR multiplier (default 1.5) or Z-score threshold (default 3)',
                default: 1.5
            }
        ],
        examples: 'Detect price outliers: field: price, method: iqr, threshold: 1.5',
        tips: 'IQR is more robust for non-normal distributions'
    },
    'StringLengthCheck': {
        icon: '📏',
        name: 'StringLength Check',
        type: 'StringLengthCheck',
        category: 'Advanced',
        description: 'Validates string field length is within bounds',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'String field to validate'
            },
            {
                name: 'min_length',
                label: 'Minimum Length',
                type: 'number',
                required: false,
                help: 'Minimum acceptable string length',
                min: 0
            },
            {
                name: 'max_length',
                label: 'Maximum Length',
                type: 'number',
                required: false,
                help: 'Maximum acceptable string length',
                min: 1
            }
        ],
        examples: 'Phone numbers: min_length: 10, max_length: 15',
        tips: 'Use to validate field sizes match database constraints'
    },
    'TrendDetectionCheck': {
        icon: '📈',
        name: 'TrendDetection Check',
        type: 'TrendDetectionCheck',
        category: 'Temporal',
        description: 'Detects trends and anomalies in time-series data',
        params: [
            {
                name: 'timestamp_field',
                label: 'Timestamp Field',
                type: 'text',
                required: true,
                help: 'Field containing timestamp'
            },
            {
                name: 'value_field',
                label: 'Value Field',
                type: 'text',
                required: true,
                help: 'Numeric field to analyze for trends'
            },
            {
                name: 'trend_type',
                label: 'Trend Type',
                type: 'select',
                required: true,
                help: 'Type of trend to detect',
                options: ['increasing', 'decreasing', 'stable', 'anomaly']
            },
            {
                name: 'sensitivity',
                label: 'Sensitivity',
                type: 'select',
                required: false,
                help: 'Detection sensitivity',
                options: ['low', 'medium', 'high'],
                default: 'medium'
            }
        ],
        examples: 'Detect anomalous sales trends: timestamp_field: date, value_field: sales',
        tips: 'Useful for monitoring KPIs and detecting unusual patterns'
    },
    'UniqueKeyCheck': {
        icon: '🔑',
        name: 'UniqueKey Check',
        type: 'UniqueKeyCheck',
        category: 'Record-Level',
        description: 'Validates specified field(s) contain unique values',
        params: [
            {
                name: 'key_fields',
                label: 'Key Fields (comma-separated)',
                type: 'text',
                required: true,
                help: 'Fields that must be unique',
                placeholder: 'transaction_id'
            }
        ],
        examples: 'Unique transaction ID: key_fields: [transaction_id]',
        tips: 'Use for single or composite primary keys'
    },
    'ValidValuesCheck': {
        icon: '✅',
        name: 'ValidValues Check',
        type: 'ValidValuesCheck',
        category: 'Field-Level',
        description: 'Validates field values are from allowed list',
        params: [
            {
                name: 'field',
                label: 'Field Name',
                type: 'text',
                required: true,
                help: 'Field to validate'
            },
            {
                name: 'valid_values',
                label: 'Valid Values (comma-separated)',
                type: 'text',
                required: true,
                help: 'List of acceptable values',
                placeholder: 'Active,Pending,Completed,Cancelled'
            },
            {
                name: 'case_sensitive',
                label: 'Case Sensitive',
                type: 'checkbox',
                required: false,
                help: 'Whether value matching is case-sensitive',
                default: true
            }
        ],
        examples: 'Status field: valid_values: [Active, Inactive, Pending]',
        tips: 'Useful for enum-like fields with fixed set of values'
    }
};