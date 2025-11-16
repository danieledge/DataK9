# Design Patterns in DataK9

**Understanding DataK9's Architecture Through Design Patterns**

DataK9 uses proven design patterns to create a clean, extensible, and maintainable architecture. Understanding these patterns helps you navigate the codebase, create custom components, and contribute effectively.

This guide explains the design patterns used in DataK9 and why they were chosen.

---

## Table of Contents

1. [Pattern Overview](#pattern-overview)
2. [Factory Pattern](#factory-pattern)
3. [Registry Pattern](#registry-pattern)
4. [Template Method Pattern](#template-method-pattern)
5. [Iterator Pattern](#iterator-pattern)
6. [Strategy Pattern](#strategy-pattern)
7. [Singleton Pattern](#singleton-pattern)
8. [Builder Pattern](#builder-pattern)
9. [Decorator Pattern](#decorator-pattern)
10. [Pattern Interactions](#pattern-interactions)

---

## Pattern Overview

DataK9 uses these design patterns:

| Pattern | Used In | Purpose |
|---------|---------|---------|
| Factory | LoaderFactory | Create appropriate loaders based on format |
| Registry | ValidationRegistry | Plugin architecture for validations |
| Template Method | ValidationRule | Define validation workflow with customization points |
| Iterator | DataLoader | Memory-efficient chunked data processing |
| Strategy | ConditionalValidation | Runtime selection of validation logic |
| Singleton | ValidationRegistry | Single global registry instance |
| Builder | ValidationEngine | Step-by-step validation job construction |
| Decorator | Validation conditions | Add conditional logic to validations |

---

## Factory Pattern

### Purpose

Create objects without specifying exact classes. DataK9 uses Factory to create appropriate loaders based on file format.

### Implementation

**LoaderFactory:**

```python
class LoaderFactory:
    """
    Factory for creating data loaders.

    Pattern: Factory Method
    Benefit: Decouples loader selection from usage
    """

    _loaders: Dict[str, Type[DataLoader]] = {
        "csv": CSVLoader,
        "excel": ExcelLoader,
        "json": JSONLoader,
        "parquet": ParquetLoader,
        "database": DatabaseLoader,
    }

    extension_map = {
        ".csv": "csv",
        ".xlsx": "excel",
        ".json": "json",
        ".parquet": "parquet",
    }

    @classmethod
    def create_loader(
        cls,
        file_path: str,
        file_format: str = None,
        **kwargs
    ) -> DataLoader:
        """
        Create appropriate loader for file.

        Pattern: Factory Method
        - Client doesn't know which class to instantiate
        - Factory makes decision based on format

        Args:
            file_path: Path to file
            file_format: Format type (csv, json, etc.)
            **kwargs: Format-specific parameters

        Returns:
            DataLoader instance

        Example:
            # Factory determines which loader to create
            loader = LoaderFactory.create_loader(
                file_path="data.csv",
                file_format="csv",
                delimiter="|"
            )
            # Returns CSVLoader instance
        """
        # Infer format from extension if not specified
        if not file_format:
            ext = Path(file_path).suffix.lower()
            file_format = cls.extension_map.get(ext, "csv")

        # Get loader class
        loader_class = cls._loaders.get(file_format)

        if not loader_class:
            raise ValueError(f"Unsupported format: {file_format}")

        # Instantiate and return
        return loader_class(file_path=file_path, **kwargs)

    @classmethod
    def register_loader(cls, format_name: str, loader_class: Type[DataLoader]):
        """
        Register custom loader.

        Pattern: Open/Closed Principle
        - Open for extension (new loaders)
        - Closed for modification (no changes to factory)
        """
        cls._loaders[format_name] = loader_class
```

### Benefits

**1. Extensibility**
```python
# Add new loader without modifying factory code
class XMLLoader(DataLoader):
    # Implementation...
    pass

LoaderFactory.register_loader("xml", XMLLoader)

# Now factory can create XML loaders
loader = LoaderFactory.create_loader("data.xml", "xml")
```

**2. Decoupling**
```python
# Client code doesn't know about specific loader classes
loader = LoaderFactory.create_loader("data.csv")
# Could be CSVLoader, but client doesn't care
```

**3. Consistency**
```python
# All loaders created through single interface
loader1 = LoaderFactory.create_loader("data.csv")
loader2 = LoaderFactory.create_loader("data.json")
# Both have same interface (load, get_metadata)
```

---

## Registry Pattern

### Purpose

Central registry for pluggable components. DataK9 uses Registry for validation types, allowing runtime discovery and instantiation.

### Implementation

**ValidationRegistry:**

```python
class ValidationRegistry:
    """
    Registry of validation types.

    Pattern: Registry (Singleton variant)
    Benefit: Central catalog of available validations
    """

    _instance = None
    _validations: Dict[str, Type[ValidationRule]] = {}

    def __new__(cls):
        """
        Singleton pattern - ensure only one registry exists.

        Pattern: Singleton
        Benefit: Global state for validation catalog
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def register(
        self,
        name: str,
        validation_class: Type[ValidationRule]
    ):
        """
        Register validation type.

        Pattern: Registry
        - Plugins register themselves
        - Runtime discovery of components

        Args:
            name: Name used in YAML configs
            validation_class: Validation class

        Example:
            # Built-in validations auto-register
            class EmailCheck(DataValidationRule):
                # Implementation...

            register_validation("EmailCheck", EmailCheck)

            # Now available via registry
            registry.get("EmailCheck")
        """
        if name in self._validations:
            logger.warning(f"Overwriting validation: {name}")

        self._validations[name] = validation_class
        logger.debug(f"Registered validation: {name}")

    def get(self, name: str) -> Type[ValidationRule]:
        """
        Get validation class by name.

        Pattern: Registry lookup
        """
        if name not in self._validations:
            raise KeyError(f"Unknown validation type: {name}")

        return self._validations[name]

    def list_validations(self) -> List[str]:
        """Get all registered validation names"""
        return sorted(self._validations.keys())

    def is_registered(self, name: str) -> bool:
        """Check if validation type exists"""
        return name in self._validations


# Global registry instance
_registry = ValidationRegistry()


def get_registry() -> ValidationRegistry:
    """Get global validation registry"""
    return _registry


def register_validation(name: str, validation_class: Type[ValidationRule]):
    """Convenience function to register validation"""
    _registry.register(name, validation_class)
```

### Usage

**Auto-registration at import:**

```python
# validation_framework/validations/field_checks.py

class MandatoryFieldCheck(DataValidationRule):
    """Check required fields are present"""
    # Implementation...

# Auto-register when module is imported
register_validation("MandatoryFieldCheck", MandatoryFieldCheck)
```

**Runtime discovery:**

```python
# List all available validations
from validation_framework.core.registry import get_registry

registry = get_registry()
all_validations = registry.list_validations()

print(f"Available validations: {', '.join(all_validations)}")
# Output: Available validations: EmptyFileCheck, MandatoryFieldCheck, RegexCheck, ...
```

### Benefits

**1. Plugin Architecture**
```python
# Add custom validation
class MyCustomCheck(DataValidationRule):
    # Implementation...

register_validation("MyCustomCheck", MyCustomCheck)

# Now available in YAML configs
# validations:
#   - type: "MyCustomCheck"
```

**2. Decoupled Discovery**
```python
# Engine doesn't need to know about all validation types
validation_class = registry.get(validation_type)
validation = validation_class(name, severity, params)
```

**3. Runtime Flexibility**
```python
# CLI can list validations without hardcoding
def list_validations_command():
    registry = get_registry()
    for name in registry.list_validations():
        print(f"- {name}")
```

---

## Template Method Pattern

### Purpose

Define skeleton of algorithm in base class, let subclasses override specific steps. DataK9 uses Template Method for validation workflow.

### Implementation

**ValidationRule base class:**

```python
class ValidationRule(ABC):
    """
    Base class for validations.

    Pattern: Template Method
    - Defines validation workflow
    - Subclasses customize specific steps
    """

    def __init__(self, name, severity, params=None, condition=None):
        self.name = name
        self.severity = severity
        self.params = params or {}
        self.condition = condition

    @abstractmethod
    def validate(
        self,
        data_iterator: Iterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        TEMPLATE METHOD: Define validation logic.

        Pattern: Template Method (abstract step)
        - Subclasses MUST implement this
        - Each validation defines its own logic
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """
        TEMPLATE METHOD: Get human-readable description.

        Pattern: Template Method (abstract step)
        - Subclasses provide their own description
        """
        pass

    def _evaluate_condition(self, df: pd.DataFrame) -> pd.Series:
        """
        HOOK METHOD: Apply conditional filter.

        Pattern: Template Method (hook)
        - Provides default behavior
        - Can be used by subclasses
        - Not meant to be overridden
        """
        if not self.condition:
            return pd.Series([True] * len(df), index=df.index)

        # Condition evaluation logic...
        return mask

    def _create_result(
        self,
        passed: bool,
        message: str,
        failed_count: int = 0,
        total_count: int = 0,
        sample_failures: list = None
    ) -> ValidationResult:
        """
        HELPER METHOD: Create validation result.

        Pattern: Template Method (helper)
        - Common logic used by all validations
        - Ensures consistent result creation
        """
        return ValidationResult(
            rule_name=self.name,
            severity=self.severity,
            passed=passed,
            message=message,
            failed_count=failed_count,
            total_count=total_count,
            sample_failures=sample_failures or []
        )
```

**Concrete implementation:**

```python
class EmailCheck(DataValidationRule):
    """
    Validate email format.

    Pattern: Template Method (concrete class)
    - Implements abstract methods
    - Uses helper methods from base class
    """

    def get_description(self) -> str:
        """IMPLEMENT: Provide description"""
        return "Validates email addresses using regex"

    def validate(
        self,
        data_iterator: Iterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        IMPLEMENT: Validation logic.

        Uses template methods:
        - _evaluate_condition() for conditional logic
        - _create_result() for result creation
        """
        field = self.params['field']
        pattern = self.params['pattern']
        failures = []

        for chunk in data_iterator:
            # Use hook method from base class
            if self.condition:
                mask = self._evaluate_condition(chunk)
                chunk = chunk[mask]

            # Custom validation logic
            invalid_emails = chunk[~chunk[field].str.match(pattern)]
            failures.extend(invalid_emails.to_dict('records'))

        # Use helper method from base class
        return self._create_result(
            passed=len(failures) == 0,
            message=f"Found {len(failures)} invalid emails",
            failed_count=len(failures),
            sample_failures=failures[:100]
        )
```

### Benefits

**1. Code Reuse**
```python
# All validations get conditional logic for free
class MyCheck(DataValidationRule):
    def validate(self, data_iterator, context):
        for chunk in data_iterator:
            # Automatically supports conditions!
            if self.condition:
                mask = self._evaluate_condition(chunk)
                chunk = chunk[mask]
            # Custom logic...
```

**2. Consistency**
```python
# All validations create results the same way
return self._create_result(
    passed=is_valid,
    message=message,
    failed_count=failures
)
```

**3. Extensibility**
```python
# Add new validation by implementing two methods
class NewCheck(DataValidationRule):
    def get_description(self):
        return "Description"

    def validate(self, data_iterator, context):
        # Custom logic
        return self._create_result(...)
```

---

## Iterator Pattern

### Purpose

Provide sequential access without exposing underlying representation. DataK9 uses Iterator for memory-efficient data processing.

### Implementation

**DataLoader returns Iterator:**

```python
class DataLoader(ABC):
    """
    Base loader.

    Pattern: Iterator
    - Returns iterator, not collection
    - Enables lazy evaluation
    """

    @abstractmethod
    def load(self) -> Iterator[pd.DataFrame]:
        """
        ITERATOR PATTERN: Return iterator of DataFrames.

        Pattern Benefits:
        - Memory efficient (one chunk at a time)
        - Lazy evaluation (process as needed)
        - Uniform interface for all data sources
        """
        pass
```

**CSVLoader implementation:**

```python
class CSVLoader(DataLoader):
    """
    CSV loader.

    Pattern: Iterator (concrete implementation)
    """

    def load(self) -> Iterator[pd.DataFrame]:
        """
        ITERATOR: Yield DataFrames one chunk at a time.

        Pattern: Iterator
        - Doesn't load entire file into memory
        - Yields chunks as needed
        - Client can process in loop
        """
        # Use pandas chunking
        for chunk in pd.read_csv(
            self.file_path,
            chunksize=self.chunk_size
        ):
            # Process chunk if needed
            yield chunk  # ITERATOR: yield, don't return
```

**Validation processes iterator:**

```python
class MandatoryFieldCheck(DataValidationRule):
    """
    Check required fields.

    Pattern: Iterator consumer
    """

    def validate(
        self,
        data_iterator: Iterator[pd.DataFrame],  # ITERATOR: receives iterator
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        ITERATOR CONSUMER: Process chunks from iterator.

        Pattern Benefits:
        - Works with any data source
        - Memory efficient
        - Scalable to huge files
        """
        failures = []

        # ITERATOR: Process one chunk at a time
        for chunk in data_iterator:
            # Check this chunk
            null_rows = chunk[chunk['email'].isnull()]
            failures.extend(null_rows.to_dict('records'))

        return self._create_result(
            passed=len(failures) == 0,
            message=f"Found {len(failures)} nulls",
            failed_count=len(failures)
        )
```

### Benefits

**1. Memory Efficiency**
```python
# Can process 200GB file with 400MB memory
loader = ParquetLoader("huge_file.parquet", chunk_size=50000)

for chunk in loader.load():
    # Only 50,000 rows in memory at once
    process(chunk)
```

**2. Lazy Evaluation**
```python
# Data not loaded until needed
loader = CSVLoader("data.csv")

# No data loaded yet (lazy)
iterator = loader.load()

# Data loaded only when iterated
for chunk in iterator:
    # Chunk loaded here, processed, then discarded
    validate(chunk)
```

**3. Uniform Interface**
```python
# All loaders return iterators
csv_data = CSVLoader("data.csv").load()
json_data = JSONLoader("data.json").load()
parquet_data = ParquetLoader("data.parquet").load()

# All processed the same way
for source_data in [csv_data, json_data, parquet_data]:
    for chunk in source_data:
        validate(chunk)
```

---

## Strategy Pattern

### Purpose

Define family of algorithms, make them interchangeable. DataK9 uses Strategy for conditional validation logic.

### Implementation

**ConditionalValidation:**

```python
class ConditionalValidation:
    """
    Apply different validation strategies based on condition.

    Pattern: Strategy
    - Encapsulates validation logic
    - Switches between strategies at runtime
    """

    def __init__(
        self,
        name: str,
        severity: Severity,
        params: dict,
        condition: str = None
    ):
        self.name = name
        self.severity = severity
        self.params = params
        self.condition = condition

        # STRATEGY: Select validation strategy based on params
        self.strategy = self._select_strategy(params)

    def _select_strategy(self, params: dict) -> ValidationStrategy:
        """
        STRATEGY SELECTOR: Choose appropriate validation strategy.

        Pattern: Strategy selection
        """
        if 'inline_condition' in params:
            return InlineConditionStrategy()
        elif 'threshold' in params:
            return ThresholdStrategy()
        elif 'custom_rule' in params:
            return CustomRuleStrategy()
        else:
            return DefaultStrategy()

    def validate(
        self,
        data_iterator: Iterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        STRATEGY EXECUTION: Delegate to selected strategy.

        Pattern: Strategy delegation
        """
        # Delegate to strategy
        return self.strategy.execute(
            data_iterator,
            self.params,
            self.condition,
            context
        )
```

**Strategy interface:**

```python
class ValidationStrategy(ABC):
    """
    Base strategy for conditional validations.

    Pattern: Strategy interface
    """

    @abstractmethod
    def execute(
        self,
        data_iterator: Iterator[pd.DataFrame],
        params: dict,
        condition: str,
        context: dict
    ) -> ValidationResult:
        """STRATEGY: Execute validation logic"""
        pass


class InlineConditionStrategy(ValidationStrategy):
    """
    Strategy for inline condition validation.

    Pattern: Concrete strategy
    """

    def execute(self, data_iterator, params, condition, context):
        """Evaluate inline pandas expression"""
        inline_condition = params['inline_condition']

        for chunk in data_iterator:
            # Apply strategy-specific logic
            violations = chunk.query(f"not ({inline_condition})")
            # ...

        return result


class ThresholdStrategy(ValidationStrategy):
    """
    Strategy for threshold-based validation.

    Pattern: Concrete strategy
    """

    def execute(self, data_iterator, params, condition, context):
        """Check values against threshold"""
        threshold = params['threshold']
        field = params['field']

        for chunk in data_iterator:
            # Different strategy logic
            violations = chunk[chunk[field] > threshold]
            # ...

        return result
```

### Benefits

**1. Runtime Selection**
```python
# Different validation logic based on params
validation1 = ConditionalValidation(
    name="Check A",
    params={'inline_condition': 'age >= 18'}
)
# Uses InlineConditionStrategy

validation2 = ConditionalValidation(
    name="Check B",
    params={'threshold': 100, 'field': 'amount'}
)
# Uses ThresholdStrategy
```

**2. Easy to Extend**
```python
# Add new strategy without modifying existing code
class PercentileStrategy(ValidationStrategy):
    def execute(self, data_iterator, params, condition, context):
        # New validation logic
        pass

# Register strategy
# No changes to ConditionalValidation needed
```

---

## Singleton Pattern

### Purpose

Ensure class has only one instance. DataK9 uses Singleton for ValidationRegistry.

### Implementation

```python
class ValidationRegistry:
    """
    Registry of validation types.

    Pattern: Singleton
    - Only one registry exists
    - Global access point
    """

    _instance = None  # Class variable holds single instance

    def __new__(cls):
        """
        SINGLETON: Ensure only one instance.

        Pattern: Override __new__ to control instantiation
        """
        if cls._instance is None:
            # First time - create instance
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False

        # Always return same instance
        return cls._instance

    def __init__(self):
        """
        SINGLETON: Initialize only once.

        Pattern: Guard initialization
        """
        if self._initialized:
            return  # Already initialized

        self._validations = {}
        self._initialized = True
```

**Usage:**

```python
# All calls return same instance
registry1 = ValidationRegistry()
registry2 = ValidationRegistry()

assert registry1 is registry2  # True - same object

# Global access function
def get_registry() -> ValidationRegistry:
    """Get the singleton registry instance"""
    return ValidationRegistry()
```

### Benefits

**1. Global State**
```python
# Register validation anywhere
register_validation("EmailCheck", EmailCheck)

# Access it anywhere else
validation_class = get_registry().get("EmailCheck")
```

**2. Consistency**
```python
# All parts of code see same validations
# No risk of multiple registries getting out of sync
```

---

## Builder Pattern

### Purpose

Construct complex objects step by step. DataK9 uses Builder for ValidationEngine creation.

### Implementation

**ValidationEngine Builder:**

```python
class ValidationEngine:
    """
    Main validation engine.

    Pattern: Builder (fluent interface variant)
    """

    @classmethod
    def from_config(cls, config_path: str) -> 'ValidationEngine':
        """
        BUILDER: Create engine from config file.

        Pattern: Builder method
        - Constructs complex engine step by step
        - Hides construction complexity
        """
        # Step 1: Load config
        config = ValidationConfig(config_path)
        config.validate()

        # Step 2: Create engine
        engine = cls()

        # Step 3: Set up job config
        job_config = config.get_job_config()
        engine.job_name = job_config.get('name')
        engine.description = job_config.get('description', '')

        # Step 4: Set up settings
        settings = config.get_settings()
        engine.chunk_size = settings.get('chunk_size', 50000)
        engine.max_sample_failures = settings.get('max_sample_failures', 100)

        # Step 5: Configure files
        engine.files_config = config.get_files_config()

        # Step 6: Return constructed engine
        return engine
```

### Benefits

**1. Hide Complexity**
```python
# Simple interface hides complex construction
engine = ValidationEngine.from_config("config.yaml")

# vs manual construction:
engine = ValidationEngine()
config = ValidationConfig("config.yaml")
config.validate()
job_config = config.get_job_config()
engine.job_name = job_config['name']
# ... many more steps
```

**2. Immutable Construction**
```python
# Engine fully configured before returned
engine = ValidationEngine.from_config("config.yaml")
# Engine ready to use immediately
```

---

## Decorator Pattern

### Purpose

Add behavior to objects dynamically. DataK9 uses Decorator for conditional validation.

### Implementation

**Conditional Logic as Decorator:**

```python
def conditional_validation(condition: str):
    """
    DECORATOR: Add conditional logic to validation.

    Pattern: Decorator
    - Wraps validation with condition check
    - Adds behavior without modifying validation
    """
    def decorator(validation_func):
        def wrapper(data_iterator, context):
            # DECORATOR: Add pre-processing
            filtered_iterator = apply_condition(data_iterator, condition)

            # Execute original validation
            result = validation_func(filtered_iterator, context)

            # DECORATOR: Add post-processing
            result.message = f"[Conditional: {condition}] {result.message}"

            return result
        return wrapper
    return decorator


# Usage
@conditional_validation("status == 'ACTIVE'")
def validate_active_users(data_iterator, context):
    # Only processes ACTIVE users
    # Decorator applied condition filter
    pass
```

**Validation with conditions:**

```python
class ValidationRule:
    """
    Validation with optional condition.

    Pattern: Decorator-like behavior
    - Condition "decorates" validation logic
    """

    def validate(self, data_iterator, context):
        # DECORATOR-LIKE: Wrap core logic with condition
        for chunk in data_iterator:
            # Apply condition (decorator behavior)
            if self.condition:
                filtered_chunk = self._apply_condition(chunk)
            else:
                filtered_chunk = chunk

            # Core validation logic
            result = self._validate_chunk(filtered_chunk)

        return result
```

---

## Pattern Interactions

### How Patterns Work Together

DataK9's patterns interact to create a cohesive system:

```python
# PATTERN INTERACTION EXAMPLE:

# 1. SINGLETON: Get registry
registry = get_registry()

# 2. REGISTRY: Look up validation type
validation_class = registry.get("EmailCheck")

# 3. FACTORY: Create validation instance
validation = validation_class(
    name="Email Validation",
    severity=Severity.ERROR,
    params={'field': 'email'}
)

# 4. FACTORY: Create loader
loader = LoaderFactory.create_loader("data.csv")

# 5. ITERATOR: Get data iterator
data_iterator = loader.load()

# 6. TEMPLATE METHOD: Execute validation
#    (uses Strategy for condition evaluation)
result = validation.validate(data_iterator, context={})

# 7. BUILDER: Create report
report = ValidationReport.builder() \
    .with_job_name("Email Validation") \
    .with_result(result) \
    .build()
```

---

## Design Principles

DataK9 follows SOLID principles:

### Single Responsibility Principle (SRP)
Each class has one reason to change:
- `CSVLoader`: Loading CSV files
- `EmailCheck`: Validating emails
- `HTMLReporter`: Generating HTML reports

### Open/Closed Principle (OCP)
Open for extension, closed for modification:
```python
# Add new validation without modifying registry
register_validation("MyCheck", MyCheck)
```

### Liskov Substitution Principle (LSP)
Subtypes are substitutable for base types:
```python
# All loaders can be used interchangeably
def process_data(loader: DataLoader):
    for chunk in loader.load():
        validate(chunk)

# Works with any loader implementation
process_data(CSVLoader("data.csv"))
process_data(JSONLoader("data.json"))
```

### Interface Segregation Principle (ISP)
Clients depend only on needed methods:
```python
# Validation only needs validate() and get_description()
# Doesn't depend on internal helpers
```

### Dependency Inversion Principle (DIP)
Depend on abstractions, not concretions:
```python
# Engine depends on DataLoader interface, not CSVLoader
class ValidationEngine:
    def validate_file(self, loader: DataLoader):
        # Depends on abstraction
        for chunk in loader.load():
            # ...
```

---

## Next Steps

- **[Architecture](architecture.md)** - See patterns in system context
- **[Custom Validations](custom-validations.md)** - Apply Template Method pattern
- **[Custom Loaders](custom-loaders.md)** - Implement Iterator pattern
- **[API Reference](api-reference.md)** - See pattern implementations

---

**🐕 Patterns guard code quality - DataK9 is built on proven foundations**
