# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in DataK9, please report it responsibly:

1. **Do not** open a public GitHub issue for security vulnerabilities
2. Email security concerns to the maintainer
3. Include details about the vulnerability and steps to reproduce

We will acknowledge receipt within 48 hours and provide a timeline for resolution.

## Security Guidelines

For complete security guidelines including:
- Credential management
- PII handling
- Database connection security
- Config file security

See **[docs/SECURITY.md](docs/SECURITY.md)**

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Best Practices

- Never hardcode credentials in YAML configs
- Use environment variables for sensitive data
- Use read-only database credentials for validation
- Add config files with credentials to `.gitignore`
