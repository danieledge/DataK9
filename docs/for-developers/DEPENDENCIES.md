# Dependencies and Licensing

Complete guide to DataK9's dependencies, licenses, and commercial use compliance.

---

## Commercial Use Compliance

**✅ ALL DEPENDENCIES ARE COMMERCIAL-USE COMPATIBLE**

Every dependency in DataK9 uses OSI-approved permissive licenses that allow:
- ✓ Commercial use without restrictions
- ✓ Modification and distribution
- ✓ Private use
- ✓ Sublicensing

**NO GPL/AGPL:** This project does NOT use copyleft licenses that would require derivative works to be open-sourced.

---

## License Summary Table

| Package | License | Commercial Use | Notes |
|---------|---------|----------------|-------|
| pandas | BSD-3-Clause | ✓ Yes | Data manipulation |
| pyarrow | Apache-2.0 | ✓ Yes | Parquet file support |
| openpyxl | MIT | ✓ Yes | Excel file support |
| PyYAML | MIT | ✓ Yes | YAML parsing |
| Jinja2 | BSD-3-Clause | ✓ Yes | HTML templating |
| Click | BSD-3-Clause | ✓ Yes | CLI framework |
| jsonschema | MIT | ✓ Yes | JSON validation |
| dask | BSD-3-Clause | ✓ Yes | Large dataset processing |
| tqdm | MPL-2.0 AND MIT | ✓ Yes | Progress bars |
| python-dateutil | Apache-2.0 OR BSD | ✓ Yes | Date parsing |
| colorama | BSD-3-Clause | ✓ Yes | Colored terminal output |
| psutil | BSD-3-Clause | ✓ Yes | System monitoring |
| statsmodels | BSD-3-Clause | ✓ Yes | Statistical analysis |
| scipy | BSD-3-Clause | ✓ Yes | Scientific computing |
| scikit-learn | BSD-3-Clause | ✓ Yes | Machine learning |
| visions | BSD-3-Clause | ✓ Yes | Type detection |
| **FIBO** | **MIT** | **✓ Yes** | **Financial ontology** |

---

## License Types Explained

### MIT License
**Used by:** openpyxl, PyYAML, jsonschema, FIBO

**Permissions:**
- ✓ Commercial use
- ✓ Modification
- ✓ Distribution
- ✓ Private use

**Requirements:**
- Include license and copyright notice

**Restrictions:**
- No liability or warranty

**Best for:** Maximum freedom, minimal requirements

---

### BSD-3-Clause License
**Used by:** pandas, Jinja2, Click, dask, colorama, psutil, statsmodels, scipy, scikit-learn, visions

**Permissions:**
- ✓ Commercial use
- ✓ Modification
- ✓ Distribution
- ✓ Private use

**Requirements:**
- Include license and copyright notice
- No use of contributor names for endorsement without permission

**Restrictions:**
- No liability or warranty

**Best for:** Similar to MIT, adds protection against name misuse

---

### Apache-2.0 License
**Used by:** pyarrow, python-dateutil (dual-licensed)

**Permissions:**
- ✓ Commercial use
- ✓ Modification
- ✓ Distribution
- ✓ Private use
- ✓ Patent grant (important!)

**Requirements:**
- Include license and copyright notice
- State changes made to the code
- Include NOTICE file if present

**Restrictions:**
- No liability or warranty
- No trademark use

**Best for:** Patent protection, enterprise deployments

**Key Advantage:** Includes explicit patent grant from contributors

---

### MPL-2.0 (Mozilla Public License)
**Used by:** tqdm (dual-licensed with MIT)

**Permissions:**
- ✓ Commercial use
- ✓ Modification
- ✓ Distribution
- ✓ Private use

**Requirements:**
- Disclose source of MPL-licensed files
- Include license and copyright notice
- Modified files must stay under MPL-2.0

**Restrictions:**
- Weak copyleft (only on modified MPL files)
- Larger work can use different license

**Note:** tqdm is dual-licensed (MPL-2.0 AND MIT), so you can use it under MIT terms instead.

---

## FIBO (Financial Industry Business Ontology)

### What is FIBO?

FIBO is an **industry-standard ontology** for financial services, maintained by the **EDM Council**.

**Official Sources:**
- Website: https://spec.edmcouncil.org/fibo/
- GitHub: https://github.com/edmcouncil/fibo
- License: MIT License

### How DataK9 Uses FIBO

DataK9's semantic tagging system uses **concepts derived from FIBO**:

**Location:** `validation_framework/profiler/taxonomies/finance_taxonomy.json`

**What We Use:**
- FIBO class names (e.g., `fibo-fnd-acc-cur:MonetaryAmount`)
- FIBO definitions (adapted for semantic tagging)
- FIBO modules (FND/Accounting, FBC/ProductsAndServices, etc.)

**What We Created:**
- Pattern matching rules for automatic detection
- Data property validation logic
- Mapping between column names and FIBO concepts

### FIBO License Compliance

**License:** MIT License

**Requirements Met:**
✓ Copyright notice included in taxonomy JSON file
✓ License information in NOTICE file
✓ Attribution in documentation
✓ Source URL provided

**Commercial Use:** ✓ **Fully permitted without restrictions**

**Attribution Example:**
```
This software uses semantic tagging derived from FIBO (Financial Industry
Business Ontology), maintained by the EDM Council under the MIT License.
```

### FIBO in HTML Reports

When semantic tagging is enabled, HTML reports include:
- FIBO class references (e.g., "fibo-fnd-acc-cur:Currency")
- Links to FIBO specification
- Definitions from FIBO (adapted)

**This is compliant:** MIT License permits derivative works and redistribution.

---

## Optional Dependencies

These are NOT required but can be installed for additional features:

### pybloom-live (Bloom Filters)
- **License:** MIT
- **Commercial Use:** ✓ Yes
- **Purpose:** 10x faster duplicate detection
- **Install:** `pip install pybloom-live`

### Database Drivers
All database drivers are commercial-use compatible:

| Driver | License | Commercial Use |
|--------|---------|----------------|
| SQLAlchemy | MIT | ✓ Yes |
| psycopg2 | LGPL-3.0 + static linking exception | ✓ Yes |
| PyMySQL | MIT | ✓ Yes |
| cx_Oracle | BSD-3-Clause | ✓ Yes |
| pyodbc | MIT | ✓ Yes |

**Note on psycopg2:** While LGPL, the **psycopg2-binary** package includes a static linking exception that permits commercial use.

### aiofiles (Async I/O)
- **License:** Apache-2.0
- **Commercial Use:** ✓ Yes
- **Purpose:** Concurrent file validation
- **Install:** `pip install aiofiles`

---

## Attribution Requirements

### When Distributing DataK9

You **MUST** include:

1. **LICENSE file** (DataK9's MIT License)
2. **NOTICE file** (third-party attributions)
3. **Copyright notices** in source files

### When Using Semantic Tagging

If you use FIBO semantic tagging features, **acknowledge FIBO**:

**In Documentation:**
```markdown
This application uses semantic tagging derived from FIBO (Financial Industry
Business Ontology), maintained by the EDM Council under the MIT License.
https://spec.edmcouncil.org/fibo/
```

**In UI/Reports:**
Already included! HTML reports automatically show:
- FIBO class references
- Links to FIBO specification
- Attribution in semantic understanding cards

---

## Commercial Deployment Checklist

### ✅ Pre-Deployment

- [ ] Review LICENSE file
- [ ] Review NOTICE file
- [ ] Include both files in distribution
- [ ] Verify all dependencies are up-to-date
- [ ] Run security audit: `pip-audit` or `safety check`
- [ ] Document which features you're using

### ✅ Legal Compliance

- [ ] Consult legal team if required
- [ ] Confirm attribution requirements are met
- [ ] Verify no GPL/AGPL dependencies introduced
- [ ] Review patent considerations (Apache-2.0 grant is protective)

### ✅ Attribution

- [ ] Keep copyright notices in source files
- [ ] Include NOTICE file in distribution
- [ ] Add FIBO attribution if using semantic tagging
- [ ] Document open source usage internally

---

## Updating Dependencies

### Check License Before Adding

```bash
# Install package
pip install new-package

# Check license
pip show new-package | grep License

# Verify commercial use is permitted
# Acceptable: MIT, BSD, Apache-2.0, ISC, PSF
# Avoid: GPL, AGPL, SSPL, proprietary
```

### Update NOTICE File

When adding dependencies, update the NOTICE file:

1. Add package to "Python Dependencies" section
2. Include copyright and license type
3. Add URL to package homepage
4. Verify commercial use compatibility

---

## Security Considerations

### Dependency Security

**Audit tools:**
```bash
# Using pip-audit (recommended)
pip install pip-audit
pip-audit

# Using safety
pip install safety
safety check
```

**GitHub Dependabot:**
- Automatically enabled on GitHub
- Creates PRs for security updates
- Review and merge promptly

### Vulnerability Response

1. Monitor security advisories
2. Update dependencies regularly
3. Test after updates
4. Document changes in CHANGELOG.md

---

## FAQ

### Q: Can I use DataK9 in commercial software?
**A:** Yes! All dependencies are commercial-use compatible.

### Q: Do I need to open-source my modifications?
**A:** No! DataK9 and all dependencies use permissive licenses.

### Q: What about FIBO licensing?
**A:** FIBO is MIT licensed. Commercial use is explicitly permitted.

### Q: Can I remove attribution?
**A:** No. You must keep the LICENSE and NOTICE files.

### Q: What about patent concerns?
**A:** pyarrow (Apache-2.0) includes patent grant. Other dependencies are safe.

### Q: Do I need to pay for commercial use?
**A:** No! Everything is free for commercial use.

### Q: Can I modify the code?
**A:** Yes! MIT license permits modifications.

### Q: Can I redistribute DataK9?
**A:** Yes! Just include LICENSE and NOTICE files.

### Q: What if I add new dependencies?
**A:** Check license, ensure commercial use compatibility, update NOTICE.

### Q: Do I need special approval?
**A:** Consult your legal team, but these are standard OSI-approved licenses.

---

## Resources

### License Texts
- MIT: https://opensource.org/licenses/MIT
- BSD-3-Clause: https://opensource.org/licenses/BSD-3-Clause
- Apache-2.0: https://opensource.org/licenses/Apache-2.0
- MPL-2.0: https://opensource.org/licenses/MPL-2.0

### FIBO Resources
- FIBO Home: https://spec.edmcouncil.org/fibo/
- FIBO GitHub: https://github.com/edmcouncil/fibo
- EDM Council: https://edmcouncil.org/

### License Compliance Tools
- FOSSA: https://fossa.com/
- Black Duck: https://www.synopsys.com/software-integrity/security-testing/software-composition-analysis.html
- pip-licenses: `pip install pip-licenses && pip-licenses`

### Questions?

For licensing questions:
- Review this document
- Check individual package licenses
- Consult with your legal team
- Open an issue: https://github.com/yourusername/data-validation-tool/issues

---

**Last Updated:** 2024-11-24
**Next Review:** Quarterly with dependency updates
