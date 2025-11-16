# Healthcare: Data Validation Examples

**DataK9 for Healthcare Data with HIPAA Compliance**

This guide provides production-ready validation examples for healthcare data. DataK9 guards your healthcare data with K9 vigilance while maintaining HIPAA compliance!

---

## Table of Contents

1. [Overview](#overview)
2. [Patient Demographics](#patient-demographics)
3. [Clinical Data](#clinical-data)
4. [Insurance Claims](#insurance-claims)
5. [Lab Results](#lab-results)
6. [Prescription Data](#prescription-data)
7. [Appointment Data](#appointment-data)
8. [EHR Integration](#ehr-integration)
9. [HIPAA Compliance Best Practices](#hipaa-compliance-best-practices)

---

## Overview

### Common Healthcare Data Challenges

- **HIPAA compliance** (Protected Health Information - PHI)
- **Data de-identification** (HIPAA Safe Harbor)
- **Clinical accuracy** (diagnosis codes, medication codes)
- **Interoperability** (HL7, FHIR standards)
- **Audit requirements** (access logs, data lineage)
- **Data quality** (completeness for care coordination)

### Healthcare Validation Priorities

1. **ERROR Severity:**
   - Missing patient identifiers
   - Invalid diagnosis/procedure codes
   - Failed referential integrity
   - PHI exposure violations

2. **WARNING Severity:**
   - Missing optional clinical data
   - Data completeness issues
   - Statistical outliers in clinical values

---

## Patient Demographics

### Scenario: Patient Master Index (PMI)

**Data:** Patient demographic data from EHR system

**Compliance:** HIPAA Privacy Rule, Security Rule

### Configuration

```yaml
validation_job:
  name: "Patient Demographics Validation"
  description: "Daily patient master data validation with HIPAA compliance"

settings:
  chunk_size: 50000
  max_sample_failures: 25  # Limited to protect PHI

files:
  - name: "patients"
    path: "data/patients_deidentified.csv"
    format: "csv"
    encoding: "utf-8"

    validations:
      # File-level checks
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      # Schema validation
      - type: "ColumnPresenceCheck"
        severity: "ERROR"
        description: "Required patient fields"
        params:
          columns:
            - "patient_id"  # Internal ID, not MRN
            - "first_name"
            - "last_name"
            - "date_of_birth"
            - "gender"
            - "zip_code_3digit"  # De-identified (first 3 digits only)
            - "registration_date"
            - "active_status"

      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Core patient identifiers"
        params:
          fields:
            - "patient_id"
            - "first_name"
            - "last_name"
            - "date_of_birth"

      # Patient ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        description: "Patient IDs must be unique"
        params:
          fields: ["patient_id"]

      # Gender validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid gender codes (HL7)"
        params:
          field: "gender"
          valid_values:
            - "M"   # Male
            - "F"   # Female
            - "O"   # Other
            - "U"   # Unknown

      # Date of birth format
      - type: "DateFormatCheck"
        severity: "ERROR"
        description: "Valid date of birth"
        params:
          field: "date_of_birth"
          format: "%Y-%m-%d"
          allow_null: false

      # Age range validation
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Patient age validation"
        params:
          field: "age"
          min_value: 0
          max_value: 120

      # ZIP code validation (first 3 digits only for HIPAA)
      - type: "RegexCheck"
        severity: "ERROR"
        description: "De-identified ZIP code (3 digits)"
        params:
          field: "zip_code_3digit"
          pattern: "^[0-9]{3}$"
          message: "ZIP code must be 3 digits (de-identified)"

      # HIPAA Safe Harbor: Verify NO full dates for ages 90+
      - type: "ConditionalValidation"
        severity: "ERROR"
        description: "HIPAA: Ages 90+ cannot have exact birth date"
        params:
          condition: "age >= 90"
          then_validate:
            - type: "RegexCheck"
              description: "Birth year only for age 90+"
              params:
                field: "date_of_birth"
                pattern: "^[0-9]{4}-00-00$"
                message: "Exact birth date prohibited for age 90+ (HIPAA)"

      # Active status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "active_status"
          valid_values: ["ACTIVE", "INACTIVE", "DECEASED"]

      # Registration date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "registration_date"
          format: "%Y-%m-%d"

      # Registration date must be before or equal to today
      - type: "FreshnessCheck"
        severity: "ERROR"
        description: "Registration date cannot be in future"
        params:
          check_type: "field_value"
          field: "registration_date"
          max_age_hours: 0  # Must be in past

      # CRITICAL: Verify NO SSN or MRN in data (HIPAA violation)
      - type: "ColumnPresenceCheck"
        severity: "ERROR"
        description: "HIPAA VIOLATION CHECK: No SSN/MRN allowed"
        params:
          columns: ["ssn", "mrn", "social_security_number", "medical_record_number"]
        # This should FAIL if any of these columns exist
```

---

## Clinical Data

### Scenario: Clinical Observations and Vital Signs

**Data:** Clinical measurements from EHR

**Standards:** LOINC, HL7 FHIR

### Configuration

```yaml
validation_job:
  name: "Clinical Data Validation"
  description: "Patient vital signs and clinical observations"

files:
  - name: "vitals"
    path: "data/patient_vitals.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "observation_id"
            - "patient_id"
            - "observation_date"
            - "observation_type"
            - "value"
            - "unit"

      # Observation ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["observation_id"]

      # Patient reference check
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        description: "Patient must exist"
        params:
          foreign_key: "patient_id"
          reference_file: "data/patients_deidentified.csv"
          reference_key: "patient_id"

      # Observation type validation (LOINC codes)
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid LOINC observation types"
        params:
          field: "observation_type"
          valid_values:
            - "8480-6"  # Systolic BP
            - "8462-4"  # Diastolic BP
            - "8867-4"  # Heart rate
            - "8310-5"  # Body temperature
            - "29463-7" # Body weight
            - "8302-2"  # Body height
            - "39156-5" # BMI
            - "2093-3"  # Cholesterol total
            - "2345-7"  # Glucose

      # Blood pressure - systolic
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Systolic BP normal range"
        params:
          field: "value"
          min_value: 60
          max_value: 250
        condition: "observation_type == '8480-6'"

      # Blood pressure - diastolic
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Diastolic BP normal range"
        params:
          field: "value"
          min_value: 40
          max_value: 180
        condition: "observation_type == '8462-4'"

      # Heart rate
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Heart rate normal range"
        params:
          field: "value"
          min_value: 30
          max_value: 250
        condition: "observation_type == '8867-4'"

      # Body temperature (Celsius)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Body temperature normal range (Celsius)"
        params:
          field: "value"
          min_value: 35.0
          max_value: 42.0
        condition: "observation_type == '8310-5' AND unit == 'Cel'"

      # Body temperature (Fahrenheit)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Body temperature normal range (Fahrenheit)"
        params:
          field: "value"
          min_value: 95.0
          max_value: 107.6
        condition: "observation_type == '8310-5' AND unit == 'degF'"

      # Weight (kg)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Body weight range (kg)"
        params:
          field: "value"
          min_value: 0.5
          max_value: 500.0
        condition: "observation_type == '29463-7' AND unit == 'kg'"

      # Height (cm)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Body height range (cm)"
        params:
          field: "value"
          min_value: 30.0
          max_value: 250.0
        condition: "observation_type == '8302-2' AND unit == 'cm'"

      # BMI
      - type: "RangeCheck"
        severity: "WARNING"
        description: "BMI range"
        params:
          field: "value"
          min_value: 10.0
          max_value: 80.0
        condition: "observation_type == '39156-5'"

      # Unit validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid UCUM units"
        params:
          field: "unit"
          valid_values:
            - "mm[Hg]"  # Blood pressure
            - "/min"     # Heart rate
            - "Cel"      # Celsius
            - "degF"     # Fahrenheit
            - "kg"       # Kilograms
            - "lb"       # Pounds
            - "cm"       # Centimeters
            - "in"       # Inches
            - "kg/m2"    # BMI
            - "mg/dL"    # Glucose, Cholesterol

      # Numeric precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "value"
          max_decimal_places: 2

      # Date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "observation_date"
          format: "%Y-%m-%d %H:%M:%S"
```

---

## Insurance Claims

### Scenario: Medical Claims Processing

**Data:** Healthcare insurance claims

**Standards:** ICD-10, CPT, HCPCS

### Configuration

```yaml
validation_job:
  name: "Insurance Claims Validation"
  description: "Medical claims validation for billing"

files:
  - name: "claims"
    path: "data/medical_claims.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "claim_id"
            - "patient_id"
            - "provider_id"
            - "service_date"
            - "diagnosis_code"
            - "procedure_code"
            - "charge_amount"
            - "claim_status"

      # Claim ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["claim_id"]

      # Patient reference
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "patient_id"
          reference_file: "data/patients_deidentified.csv"
          reference_key: "patient_id"

      # ICD-10 diagnosis code format
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid ICD-10 code format"
        params:
          field: "diagnosis_code"
          pattern: "^[A-Z][0-9]{2}\\.?[0-9A-Z]{0,4}$"
          message: "Invalid ICD-10 code format"

      # CPT procedure code format (5 digits)
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid CPT code format"
        params:
          field: "procedure_code"
          pattern: "^[0-9]{5}$"
          message: "CPT code must be 5 digits"

      # Charge amount validation
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Charge amount must be positive"
        params:
          field: "charge_amount"
          min_value: 0.01
          max_value: 1000000.00

      # Charge amount precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "charge_amount"
          max_decimal_places: 2

      # Claim status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "claim_status"
          valid_values:
            - "SUBMITTED"
            - "PENDING"
            - "APPROVED"
            - "DENIED"
            - "PARTIALLY_PAID"
            - "PAID"
            - "APPEALED"

      # Service date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "service_date"
          format: "%Y-%m-%d"

      # Service date cannot be in future
      - type: "FreshnessCheck"
        severity: "ERROR"
        description: "Service date cannot be in future"
        params:
          check_type: "field_value"
          field: "service_date"
          max_age_hours: 0

      # Conditional: Denied claims need denial reason
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Denied claims must have denial reason"
        params:
          fields: ["denial_reason", "denial_code"]
        condition: "claim_status == 'DENIED'"

      # Outlier detection for charges
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect unusual charge amounts"
        params:
          field: "charge_amount"
          method: "zscore"
          threshold: 3.0
```

---

## Lab Results

### Scenario: Laboratory Test Results

**Data:** Lab results from LIMS

**Standards:** LOINC, HL7

### Configuration

```yaml
validation_job:
  name: "Lab Results Validation"
  description: "Laboratory test results validation"

files:
  - name: "lab_results"
    path: "data/lab_results.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "result_id"
            - "patient_id"
            - "test_code"
            - "test_name"
            - "result_value"
            - "result_unit"
            - "reference_range"
            - "result_status"
            - "collection_date"
            - "result_date"

      # Result ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["result_id"]

      # Test code format (LOINC)
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid LOINC code format"
        params:
          field: "test_code"
          pattern: "^[0-9]{4,5}-[0-9]$"
          message: "LOINC code format: XXXXX-X"

      # Result status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "result_status"
          valid_values:
            - "PRELIMINARY"
            - "FINAL"
            - "CORRECTED"
            - "CANCELLED"

      # Common lab test ranges
      # Hemoglobin (g/dL)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Hemoglobin normal range"
        params:
          field: "result_value"
          min_value: 5.0
          max_value: 20.0
        condition: "test_code == '718-7' AND result_unit == 'g/dL'"

      # Glucose (mg/dL)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Glucose normal range"
        params:
          field: "result_value"
          min_value: 30.0
          max_value: 600.0
        condition: "test_code == '2345-7' AND result_unit == 'mg/dL'"

      # White Blood Cell Count (10*9/L)
      - type: "RangeCheck"
        severity: "WARNING"
        description: "WBC normal range"
        params:
          field: "result_value"
          min_value: 1.0
          max_value: 50.0
        condition: "test_code == '6690-2' AND result_unit == '10*9/L'"

      # Collection date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "collection_date"
          format: "%Y-%m-%d %H:%M:%S"

      # Result date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "result_date"
          format: "%Y-%m-%d %H:%M:%S"

      # Result date must be after collection date
      - type: "CrossFieldComparisonCheck"
        severity: "ERROR"
        description: "Result date must be after collection date"
        params:
          field1: "collection_date"
          field2: "result_date"
          operator: "<="

      # Critical values need mandatory alert
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Critical results need alert documentation"
        params:
          fields: ["critical_alert_sent", "alert_timestamp", "acknowledged_by"]
        condition: "critical_flag == 'YES'"
```

---

## Prescription Data

### Scenario: E-Prescribing Data

**Data:** Electronic prescriptions

**Standards:** RxNorm, NDC

### Configuration

```yaml
validation_job:
  name: "Prescription Validation"
  description: "E-prescribing data validation"

files:
  - name: "prescriptions"
    path: "data/prescriptions.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "prescription_id"
            - "patient_id"
            - "prescriber_id"
            - "drug_name"
            - "drug_code"  # RxNorm or NDC
            - "dosage"
            - "quantity"
            - "refills"
            - "prescribed_date"
            - "status"

      # Prescription ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["prescription_id"]

      # Patient reference
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "patient_id"
          reference_file: "data/patients_deidentified.csv"
          reference_key: "patient_id"

      # NDC code format (11 digits: 5-4-2 or other variations)
      - type: "RegexCheck"
        severity: "WARNING"
        description: "NDC code format"
        params:
          field: "drug_code"
          pattern: "^[0-9]{11}$"
          message: "NDC should be 11 digits"

      # Quantity validation
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Quantity must be positive"
        params:
          field: "quantity"
          min_value: 1
          max_value: 1000

      # Refills validation
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Refills range"
        params:
          field: "refills"
          min_value: 0
          max_value: 12

      # Status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "status"
          valid_values:
            - "ACTIVE"
            - "DISCONTINUED"
            - "COMPLETED"
            - "CANCELLED"
            - "ON_HOLD"

      # Prescribed date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "prescribed_date"
          format: "%Y-%m-%d"

      # Controlled substances need DEA number
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Controlled substances need DEA number"
        params:
          fields: ["prescriber_dea_number"]
        condition: "controlled_substance == 'YES'"

      # DEA number format
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid DEA number format"
        params:
          field: "prescriber_dea_number"
          pattern: "^[A-Z][A-Z0-9]{8}$"
        condition: "controlled_substance == 'YES'"
```

---

## Appointment Data

### Scenario: Patient Appointments

**Data:** Appointment scheduling data

### Configuration

```yaml
validation_job:
  name: "Appointment Data Validation"
  description: "Patient appointment validation"

files:
  - name: "appointments"
    path: "data/appointments.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "appointment_id"
            - "patient_id"
            - "provider_id"
            - "appointment_date"
            - "appointment_time"
            - "appointment_type"
            - "status"

      # Appointment ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["appointment_id"]

      # Patient reference
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "patient_id"
          reference_file: "data/patients_deidentified.csv"
          reference_key: "patient_id"

      # Appointment type validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "appointment_type"
          valid_values:
            - "OFFICE_VISIT"
            - "TELEHEALTH"
            - "PROCEDURE"
            - "FOLLOW_UP"
            - "CONSULTATION"
            - "ANNUAL_PHYSICAL"

      # Status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "status"
          valid_values:
            - "SCHEDULED"
            - "CONFIRMED"
            - "CHECKED_IN"
            - "IN_PROGRESS"
            - "COMPLETED"
            - "CANCELLED"
            - "NO_SHOW"

      # Date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "appointment_date"
          format: "%Y-%m-%d"

      # Time format (24-hour)
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid 24-hour time format"
        params:
          field: "appointment_time"
          pattern: "^([01][0-9]|2[0-3]):[0-5][0-9]$"

      # No-shows require reason
      - type: "MandatoryFieldCheck"
        severity: "WARNING"
        description: "No-shows should have reason documented"
        params:
          fields: ["no_show_reason"]
        condition: "status == 'NO_SHOW'"
```

---

## EHR Integration

### Scenario: HL7 Message Validation

**Data:** HL7 v2 ADT messages converted to CSV

**Standards:** HL7 v2.x

### Configuration

```yaml
validation_job:
  name: "HL7 ADT Message Validation"
  description: "HL7 ADT (Admission/Discharge/Transfer) validation"

files:
  - name: "hl7_adt"
    path: "data/hl7_adt_messages.csv"

    validations:
      # Mandatory HL7 segments
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Required HL7 ADT fields"
        params:
          fields:
            - "message_id"
            - "message_type"  # ADT^A01, ADT^A02, etc.
            - "patient_id"
            - "visit_number"
            - "event_timestamp"
            - "sending_facility"
            - "receiving_application"

      # Message type validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid HL7 ADT message types"
        params:
          field: "message_type"
          valid_values:
            - "ADT^A01"  # Admit
            - "ADT^A02"  # Transfer
            - "ADT^A03"  # Discharge
            - "ADT^A04"  # Register
            - "ADT^A08"  # Update
            - "ADT^A11"  # Cancel admit
            - "ADT^A13"  # Cancel discharge

      # Event timestamp format (HL7: YYYYMMDDHHMMSS)
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid HL7 timestamp format"
        params:
          field: "event_timestamp"
          pattern: "^[0-9]{14}$"
          message: "HL7 timestamp: YYYYMMDDHHMMSS"

      # Conditional: Discharge requires discharge date
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Discharge messages need discharge date"
        params:
          fields: ["discharge_date", "discharge_disposition"]
        condition: "message_type == 'ADT^A03'"
```

---

## HIPAA Compliance Best Practices

### 1. Data De-Identification

**Always de-identify PHI in validation data:**

```yaml
# ✅ CORRECT - De-identified
params:
  columns:
    - "patient_id"      # Internal ID, not MRN
    - "zip_code_3digit" # First 3 digits only
    - "birth_year"      # Year only for age 90+

# ❌ WRONG - Contains PHI
params:
  columns:
    - "mrn"             # Medical Record Number
    - "ssn"             # Social Security Number
    - "full_zip_code"   # Full ZIP
```

### 2. Minimum Necessary Rule

**Only validate fields needed for purpose:**

```yaml
# Only include fields necessary for validation
columns:
  - "patient_id"
  - "diagnosis_code"
  - "service_date"
# Don't include unnecessary PHI
```

### 3. Audit Logging

**Track all data access:**

```bash
# Log all validation runs
python3 -m validation_framework.cli validate config.yaml \
    --verbose \
    2>&1 | tee "/var/log/hipaa/validation_$(date +%Y%m%d_%H%M%S).log"
```

### 4. Encryption

**Encrypt data at rest and in transit:**

```bash
# Encrypt input files
gpg --encrypt --recipient data-team@hospital.org patients.csv

# Decrypt before validation
gpg --decrypt patients.csv.gpg > /tmp/patients.csv

# Validate
python3 -m validation_framework.cli validate config.yaml

# Securely delete temp file
shred -u /tmp/patients.csv
```

### 5. Access Controls

**Limit who can run validations:**

```bash
# Restrict file permissions
chmod 600 config.yaml
chmod 600 data/*.csv

# Run with specific user
sudo -u hipaa-validation python3 -m validation_framework.cli validate config.yaml
```

### 6. Limited Sample Failures

**Minimize PHI exposure in reports:**

```yaml
settings:
  max_sample_failures: 10  # Limit PHI in reports
```

### 7. Secure Report Storage

**Store reports securely:**

```bash
# Encrypted report directory
python3 -m validation_framework.cli validate config.yaml \
    --output-dir /encrypted/hipaa-reports/

# Set strict permissions
chmod 700 /encrypted/hipaa-reports/
```

---

## Next Steps

1. **[Finance Examples](finance.md)** - Financial data validation
2. **[E-Commerce Examples](ecommerce.md)** - Customer and order validation
3. **[Best Practices](../using-datak9/best-practices.md)** - Production patterns

---

**🐕 DataK9 for Healthcare - Your HIPAA-compliant K9 guardian for healthcare data quality**
