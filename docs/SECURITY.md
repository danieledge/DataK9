# Security Guidelines

Best practices for using DataK9 securely with sensitive data.

---

## Credential Management

### Database Connections

**Never hardcode credentials in YAML configs:**

```yaml
# BAD - credentials in config file
database:
  connection_string: "postgresql://admin:password123@localhost/mydb"

# GOOD - use environment variables
database:
  connection_string: "${DB_CONNECTION_STRING}"
```

**Use environment variables:**

```bash
export DB_CONNECTION_STRING="postgresql://user:pass@host/db"
python3 -m validation_framework.cli validate config.yaml
```

**Use secrets managers in production:**
- AWS Secrets Manager
- HashiCorp Vault
- Azure Key Vault
- GCP Secret Manager

### Config File Security

- Add `*.yaml` configs with credentials to `.gitignore`
- Use separate config files for sensitive settings
- Set restrictive file permissions: `chmod 600 config.yaml`

---

## PII Handling

### DataK9 PII Detection

The profiler automatically detects common PII patterns:
- Email addresses
- Phone numbers
- Social Security Numbers (SSN)
- Credit card numbers
- Account numbers

**Review profiler reports for PII warnings before sharing.**

### Best Practices

1. **Mask PII in reports** - Don't include full PII values in validation failure samples
2. **Limit sample failures** - Reduce `max_sample_failures` to minimize exposed data:
   ```yaml
   settings:
     max_sample_failures: 5  # Fewer samples = less exposure
   ```
3. **Secure report storage** - Store HTML/JSON reports in access-controlled locations
4. **Delete temporary files** - Clean up profiler outputs after review

### Data Minimization

Only validate necessary fields:

```yaml
validations:
  # Validate format without storing values
  - type: "RegexCheck"
    params:
      field: "ssn"
      pattern: "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"
```

---

## Report Security

### HTML Reports

- Reports may contain sample data from validation failures
- Store reports in secure, access-controlled locations
- Do not commit reports to public repositories
- Consider using JSON output for programmatic processing (easier to filter sensitive data)

### JSON Output

Filter sensitive fields before sharing:

```bash
# Generate JSON report
python3 -m validation_framework.cli validate config.yaml -j results.json

# Filter before sharing (example using jq)
jq 'del(.sample_failures)' results.json > results_safe.json
```

---

## Production Deployment

### File Permissions

```bash
# Config files (contain potential secrets)
chmod 600 config.yaml

# Output directories
chmod 700 reports/
```

### Network Security

- Use TLS for database connections
- Validate SSL certificates
- Use network isolation where possible

### Logging

DataK9 logging does not include:
- Full row data from validations
- Database credentials
- Complete PII values

However, file paths and column names are logged. Review log destinations for sensitivity.

---

## Vulnerability Reporting

If you discover a security vulnerability in DataK9:

1. **Do not** open a public GitHub issue
2. Email security concerns to the maintainer directly
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

We aim to respond to security reports within 48 hours.

---

## Compliance Considerations

DataK9 can help with compliance but is not a compliance solution itself.

### GDPR
- Use PII detection to identify personal data
- Implement data minimization in validation configs
- Secure report storage and access

### HIPAA
- Encrypt data at rest and in transit
- Implement access controls for PHI validation
- Audit log access to validation reports

### SOX
- Use CDA gap analysis for critical financial fields
- Document validation coverage for audit trails
- Implement change management for validation configs

### PCI DSS
- Never store full credit card numbers in reports
- Use regex validation for format checking only
- Secure access to validation infrastructure

---

## Security Checklist

Before production deployment:

- [ ] Credentials stored in environment variables or secrets manager
- [ ] Config files excluded from version control
- [ ] File permissions restricted appropriately
- [ ] Database connections use TLS
- [ ] Report storage is access-controlled
- [ ] PII detection reviewed in profiler output
- [ ] Sample failure count minimized
- [ ] Logging destinations secured
- [ ] Team trained on security practices

---

**See Also:**
- [Database Credentials Security](guides/database/DATABASE_CREDENTIALS_SECURITY.md)
- [Database Safety](guides/database/DATABASE_SAFETY.md)
- [Best Practices](using-datak9/best-practices.md)
