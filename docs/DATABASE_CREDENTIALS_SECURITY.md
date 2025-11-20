# Database Credentials Security Guide

Complete guide to securely storing and managing database credentials for DataK9 validations.

**Author:** Daniel Edge
**Last Updated:** 2025-11-20

---

## Table of Contents

- [Overview](#overview)
- [Security Best Practices](#security-best-practices)
- [Method 1: Environment Variables](#method-1-environment-variables)
- [Method 2: .env Files with dotenv](#method-2-env-files-with-dotenv)
- [Method 3: Linux Secrets Manager (systemd credentials)](#method-3-linux-secrets-manager-systemd-credentials)
- [Method 4: HashiCorp Vault](#method-4-hashicorp-vault)
- [Method 5: AWS Secrets Manager](#method-5-aws-secrets-manager)
- [Method 6: Encrypted Configuration Files](#method-6-encrypted-configuration-files)
- [Method 7: SSH Tunneling](#method-7-ssh-tunneling)
- [Comparison Matrix](#comparison-matrix)
- [Common Pitfalls to Avoid](#common-pitfalls-to-avoid)

---

## Overview

Database credentials should NEVER be hardcoded in configuration files or code. This guide provides secure methods to store and access database connection strings for DataK9 validations.

**Risk Levels:**
- ❌ **CRITICAL:** Credentials in Git repositories
- ⚠️ **HIGH:** Credentials in plain text files
- ⚠️ **MEDIUM:** Credentials in environment variables (server-side only)
- ✅ **LOW:** Credentials in secrets managers
- ✅ **MINIMAL:** Credentials with encryption + rotation + auditing

---

## Security Best Practices

### Golden Rules

1. **Never commit credentials to Git**
   - Add to `.gitignore`: `*.env`, `*.credentials`, `*secret*`
   - Use placeholder values in example configs

2. **Use least privilege access**
   - Read-only database users for validation
   - Restrict to specific tables/schemas
   - Set connection limits

3. **Rotate credentials regularly**
   - Monthly for production databases
   - Immediately after exposure

4. **Audit credential access**
   - Log who accesses secrets and when
   - Monitor for unauthorized access

5. **Encrypt at rest and in transit**
   - Use SSL/TLS for database connections
   - Encrypt credential storage files

---

## Method 1: Environment Variables

**Security Level:** ⚠️ MEDIUM (server-side), ❌ CRITICAL (desktop/laptop)

### Pros
- Simple to implement
- No external dependencies
- Works across all environments

### Cons
- Visible in process listings (`ps aux`)
- Stored in shell history
- Leaked through error messages
- Not encrypted at rest

### Implementation

#### Set Environment Variable (Current Session)

```bash
# Set database connection string
export DB_CONNECTION="postgresql://user:password@localhost:5432/mydb"

# Verify (DON'T do this on shared systems!)
echo $DB_CONNECTION

# Use in DataK9 validation
python3 -m validation_framework.cli validate config.yaml
```

#### Set Environment Variable (Persistent)

```bash
# Add to ~/.bashrc (for bash)
echo 'export DB_CONNECTION="postgresql://user:password@localhost:5432/mydb"' >> ~/.bashrc
source ~/.bashrc

# Add to ~/.zshrc (for zsh)
echo 'export DB_CONNECTION="postgresql://user:password@localhost:5432/mydb"' >> ~/.zshrc
source ~/.zshrc

# Add to ~/.profile (system-wide, runs on login)
echo 'export DB_CONNECTION="postgresql://user:password@localhost:5432/mydb"' >> ~/.profile
```

#### Use in DataK9 Config

```yaml
validation_job:
  job_name: "Database Validation"
  files:
    - name: "production_db"
      format: database
      connection_string: "${DB_CONNECTION}"  # References environment variable
      table_name: "customers"
      validations:
        - type: RowCountRangeCheck
          params:
            min_rows: 100
```

#### Secure Deletion from History

```bash
# Clear current session history
history -c

# Clear bash history file
cat /dev/null > ~/.bash_history

# Prevent command from being saved to history (prepend with space)
 export DB_CONNECTION="postgresql://user:password@localhost:5432/mydb"
```

---

## Method 2: .env Files with dotenv

**Security Level:** ⚠️ MEDIUM (with proper permissions)

### Pros
- Separate credentials from code
- Easy to manage multiple environments
- IDE support for .env files
- Can be encrypted at rest

### Cons
- Plain text file (if not encrypted)
- Risk of accidental Git commit
- Requires library dependency

### Implementation

#### Install python-dotenv

```bash
pip install python-dotenv
```

#### Create .env File

```bash
# Create .env file
cat > .env << 'EOF'
# Database Credentials (NEVER commit to Git!)
DB_CONNECTION_PROD="postgresql://user:password@prod-host:5432/mydb"
DB_CONNECTION_DEV="postgresql://user:password@localhost:5432/test_db"
DB_CONNECTION_STAGING="postgresql://user:password@staging-host:5432/staging_db"

# Other sensitive values
API_KEY="your-api-key-here"
EOF

# Set strict permissions (owner read/write only)
chmod 600 .env

# Verify permissions
ls -la .env
# Should show: -rw------- (600)
```

#### Add to .gitignore

```bash
# Add .env to .gitignore
echo ".env" >> .gitignore
echo "*.env" >> .gitignore
echo ".env.*" >> .gitignore

# Create example file for developers
cat > .env.example << 'EOF'
# Database Credentials
DB_CONNECTION_PROD="postgresql://user:password@host:5432/dbname"
DB_CONNECTION_DEV="postgresql://user:password@localhost:5432/test_db"
EOF
```

#### Load in Python Script

```python
#!/usr/bin/env python3
"""
Load database credentials from .env file.
"""
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access credentials
db_connection = os.getenv('DB_CONNECTION_PROD')

# Use in validation
from validation_framework.core.config import ValidationConfig

config = ValidationConfig.from_yaml('config.yaml')
# Connection string from .env will be used
```

#### Encrypt .env File

```bash
# Encrypt .env file with GPG
gpg --symmetric --cipher-algo AES256 .env
# This creates .env.gpg (encrypted)

# Decrypt when needed
gpg --decrypt .env.gpg > .env

# Or decrypt to memory and export
eval $(gpg --decrypt .env.gpg | grep -v '^#' | sed 's/^/export /')
```

---

## Method 3: Linux Secrets Manager (systemd credentials)

**Security Level:** ✅ LOW

Modern Linux systems with systemd (v247+) have built-in credentials management.

### Implementation

#### Store Credential

```bash
# Create credentials directory
sudo mkdir -p /etc/credstore

# Store database connection string
echo -n "postgresql://user:password@localhost:5432/mydb" | \
    sudo systemd-creds encrypt --name=db_connection - /etc/credstore/db_connection.cred

# Set permissions
sudo chmod 600 /etc/credstore/db_connection.cred
sudo chown root:root /etc/credstore/db_connection.cred
```

#### Retrieve Credential

```bash
# Decrypt and use credential
DB_CONNECTION=$(sudo systemd-creds decrypt /etc/credstore/db_connection.cred)
export DB_CONNECTION

# Use in validation
python3 -m validation_framework.cli validate config.yaml
```

#### Create Systemd Service (Optional)

```bash
# Create service file
sudo tee /etc/systemd/system/datak9-validation.service << 'EOF'
[Unit]
Description=DataK9 Validation Service
After=network.target

[Service]
Type=oneshot
User=datak9
Group=datak9
LoadCredential=db_connection:/etc/credstore/db_connection.cred
ExecStart=/usr/bin/python3 -m validation_framework.cli validate /etc/datak9/config.yaml
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
sudo systemctl daemon-reload

# Run validation
sudo systemctl start datak9-validation

# View logs
sudo journalctl -u datak9-validation
```

---

## Method 4: HashiCorp Vault

**Security Level:** ✅ MINIMAL

Enterprise-grade secrets management with auditing, encryption, and rotation.

### Installation

```bash
# Download Vault
wget https://releases.hashicorp.com/vault/1.15.0/vault_1.15.0_linux_amd64.zip
unzip vault_1.15.0_linux_amd64.zip
sudo mv vault /usr/local/bin/

# Verify installation
vault --version
```

### Setup Vault Server

```bash
# Start Vault dev server (for testing only!)
vault server -dev &

# Set environment variables
export VAULT_ADDR='http://127.0.0.1:8200'
export VAULT_TOKEN='dev-token'  # Use real token in production

# Store database credential
vault kv put secret/database/prod \
    connection_string="postgresql://user:password@localhost:5432/mydb"

# Retrieve credential
vault kv get -field=connection_string secret/database/prod
```

### Use in Python

```bash
# Install Vault client
pip install hvac
```

```python
import hvac
import os

# Connect to Vault
client = hvac.Client(
    url=os.getenv('VAULT_ADDR'),
    token=os.getenv('VAULT_TOKEN')
)

# Retrieve secret
secret = client.secrets.kv.v2.read_secret_version(
    path='database/prod',
    mount_point='secret'
)

db_connection = secret['data']['data']['connection_string']
os.environ['DB_CONNECTION'] = db_connection
```

---

## Method 5: AWS Secrets Manager

**Security Level:** ✅ MINIMAL

For AWS-hosted applications.

### Prerequisites

```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Configure AWS credentials
aws configure
```

### Store Secret

```bash
# Store database connection string
aws secretsmanager create-secret \
    --name prod/database/connection \
    --description "Production database connection string" \
    --secret-string "postgresql://user:password@prod-host:5432/mydb"

# Retrieve secret
aws secretsmanager get-secret-value \
    --secret-id prod/database/connection \
    --query SecretString \
    --output text
```

### Use in Python

```bash
pip install boto3
```

```python
import boto3
import json

# Create secrets manager client
client = boto3.client('secretsmanager', region_name='us-east-1')

# Retrieve secret
response = client.get_secret_value(SecretId='prod/database/connection')
db_connection = response['SecretString']

# Use in validation
os.environ['DB_CONNECTION'] = db_connection
```

---

## Method 6: Encrypted Configuration Files

**Security Level:** ✅ LOW

### Using GPG Encryption

```bash
# Create config file with credentials
cat > database_config.yaml << 'EOF'
production:
  connection_string: "postgresql://user:password@prod-host:5432/mydb"
  max_rows: 10000
staging:
  connection_string: "postgresql://user:password@staging-host:5432/staging_db"
  max_rows: 5000
EOF

# Encrypt with GPG
gpg --symmetric --cipher-algo AES256 database_config.yaml
# This creates database_config.yaml.gpg

# Remove plaintext
shred -u database_config.yaml

# Decrypt when needed
gpg --decrypt database_config.yaml.gpg > /tmp/database_config.yaml

# Use in validation
python3 -m validation_framework.cli validate config.yaml

# Clean up
shred -u /tmp/database_config.yaml
```

### Using OpenSSL Encryption

```bash
# Encrypt file
openssl enc -aes-256-cbc -salt -in database_config.yaml \
    -out database_config.yaml.enc -pbkdf2

# Decrypt file
openssl enc -aes-256-cbc -d -in database_config.yaml.enc \
    -out /tmp/database_config.yaml -pbkdf2

# Clean up
shred -u /tmp/database_config.yaml
```

---

## Method 7: SSH Tunneling

**Security Level:** ✅ LOW (for remote databases)

Connect to remote databases securely through SSH tunnel.

### Setup SSH Tunnel

```bash
# Create SSH tunnel to remote database
ssh -f -N -L 5432:localhost:5432 user@remote-db-host

# Now connect to localhost:5432 which tunnels to remote database
DB_CONNECTION="postgresql://user:password@localhost:5432/mydb"

# Use in validation
python3 -m validation_framework.cli validate config.yaml

# Close tunnel
pkill -f "ssh -f -N -L 5432"
```

### Persistent SSH Tunnel

```bash
# Create systemd service for SSH tunnel
sudo tee /etc/systemd/system/db-tunnel.service << 'EOF'
[Unit]
Description=SSH Tunnel to Production Database
After=network.target

[Service]
User=datak9
ExecStart=/usr/bin/ssh -N -L 5432:localhost:5432 user@remote-db-host
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start
sudo systemctl enable db-tunnel
sudo systemctl start db-tunnel
```

---

## Comparison Matrix

| Method | Security | Ease of Use | Cost | Best For |
|--------|----------|-------------|------|----------|
| Environment Variables | ⚠️ Medium | ⭐⭐⭐⭐⭐ Easy | Free | Development/Testing |
| .env Files | ⚠️ Medium | ⭐⭐⭐⭐ Easy | Free | Local Development |
| Systemd Credentials | ✅ Low | ⭐⭐⭐ Moderate | Free | Linux Servers |
| HashiCorp Vault | ✅ Minimal | ⭐⭐ Complex | Free/Paid | Enterprise |
| AWS Secrets Manager | ✅ Minimal | ⭐⭐⭐ Moderate | $0.40/secret/month | AWS Deployments |
| Encrypted Files | ✅ Low | ⭐⭐ Moderate | Free | Offline/Airgapped |
| SSH Tunneling | ✅ Low | ⭐⭐⭐ Moderate | Free | Remote Databases |

---

## Common Pitfalls to Avoid

### ❌ DON'T: Hardcode Credentials

```yaml
# BAD - Never do this!
files:
  - name: "production_db"
    connection_string: "postgresql://admin:MyPassword123@prod-db:5432/mydb"
```

### ✅ DO: Use Environment Variables

```yaml
# GOOD - Use environment variable references
files:
  - name: "production_db"
    connection_string: "${DB_CONNECTION}"
```

### ❌ DON'T: Commit .env Files

```bash
# BAD - .env file in Git history
git add .env
git commit -m "Add database config"
```

### ✅ DO: Add to .gitignore

```bash
# GOOD - Exclude from Git
echo ".env" >> .gitignore
git add .gitignore
git commit -m "Add .gitignore for sensitive files"
```

### ❌ DON'T: Use World-Readable Permissions

```bash
# BAD - Everyone can read credentials
chmod 644 .env
```

### ✅ DO: Set Restrictive Permissions

```bash
# GOOD - Only owner can read/write
chmod 600 .env
chown $USER:$USER .env
```

### ❌ DON'T: Store Credentials in Error Messages

```python
# BAD - Exposes credentials in logs
try:
    conn = psycopg2.connect(connection_string)
except Exception as e:
    logger.error(f"Failed to connect with: {connection_string}")  # DON'T!
```

### ✅ DO: Sanitize Error Messages

```python
# GOOD - Hide sensitive details
try:
    conn = psycopg2.connect(connection_string)
except Exception as e:
    logger.error(f"Database connection failed: {type(e).__name__}")
```

---

## Recommended Approach by Environment

### Development (Local Machine)
1. `.env` file with `chmod 600`
2. Never commit to Git
3. Use read-only database users

### Staging/Testing
1. Environment variables set by CI/CD
2. Separate credentials from production
3. Limited table access

### Production
1. **Best:** HashiCorp Vault or AWS Secrets Manager
2. **Good:** systemd credentials with encryption
3. **Minimum:** Encrypted .env files with restrictive permissions
4. Read-only database users
5. IP whitelisting
6. SSL/TLS required
7. Connection pooling limits
8. Credential rotation every 30 days

---

## Quick Reference Commands

```bash
# Set environment variable (current session)
export DB_CONNECTION="postgresql://user:pass@host:5432/db"

# Set environment variable (permanent)
echo 'export DB_CONNECTION="..."' >> ~/.bashrc

# Create .env file
cat > .env << 'EOF'
DB_CONNECTION="postgresql://user:pass@host:5432/db"
EOF
chmod 600 .env

# Encrypt with GPG
gpg --symmetric --cipher-algo AES256 credentials.txt

# Decrypt with GPG
gpg --decrypt credentials.txt.gpg

# Check file permissions
ls -la .env

# Set restrictive permissions
chmod 600 .env
chown $USER:$USER .env

# Clear bash history
history -c && cat /dev/null > ~/.bash_history

# Create SSH tunnel
ssh -f -N -L 5432:localhost:5432 user@remote-host

# Vault quick start
vault kv put secret/db connection="postgresql://..."
vault kv get -field=connection secret/db

# AWS Secrets Manager
aws secretsmanager get-secret-value --secret-id mydb --query SecretString
```

---

## Resources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [HashiCorp Vault Documentation](https://www.vaultproject.io/docs)
- [AWS Secrets Manager Documentation](https://docs.aws.amazon.com/secretsmanager/)
- [systemd Credentials Documentation](https://www.freedesktop.org/software/systemd/man/systemd-creds.html)
- [Python dotenv Documentation](https://pypi.org/project/python-dotenv/)

---

**Remember:** The best security practice is defense in depth. Use multiple layers:
1. Encrypted storage
2. Restricted file permissions
3. Environment isolation
4. Least privilege access
5. Regular rotation
6. Comprehensive auditing

Never compromise on credential security!
