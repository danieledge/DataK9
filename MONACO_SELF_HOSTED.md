# Self-Hosted Monaco Editor

DataK9 Studio now includes a **self-hosted version of Monaco Editor** for corporate environments where external CDN access may be blocked.

## Overview

**Monaco Editor** is the code editor that powers VS Code. DataK9 Studio uses it for YAML editing with syntax highlighting, auto-completion, and validation.

### Previous Version (CDN-based)
- Loaded from: `https://cdn.jsdelivr.net/npm/monaco-editor@0.45.0/`
- **Pros:** Small bundle size (170KB)
- **Cons:** Requires internet, blocked in some corporate environments

### Current Version (Self-Hosted)
- Loaded from: `monaco-editor/min/vs/` (local directory)
- **Pros:** Works offline, no external dependencies, corporate firewall friendly
- **Cons:** Larger bundle size (~13MB total)

## Directory Structure

```
data-validation-tool/
├── datak9-studio.html           # Main application (208KB)
└── monaco-editor/               # Monaco Editor (13MB)
    └── min/
        └── vs/
            ├── loader.js        # AMD module loader
            ├── editor/          # Editor core
            ├── base/            # Base functionality
            ├── basic-languages/ # Language support (YAML, JSON, etc.)
            └── language/        # Language services
```

## File Sizes

| Component | Size | Purpose |
|-----------|------|---------|
| `datak9-studio.html` | 208KB | Main application |
| `monaco-editor/` | 13MB | Code editor engine |
| **Total** | **~13.2MB** | Complete self-contained package |

## Benefits for Corporate Environments

### ✅ No Internet Required
- Works completely offline
- No external CDN dependencies
- Air-gapped environment compatible

### ✅ Firewall Friendly
- No blocked external requests
- All resources served locally
- No corporate proxy issues

### ✅ Security & Privacy
- No data sent to external services
- No tracking or analytics from CDN
- Complete data sovereignty

### ✅ Reliability
- No CDN downtime concerns
- Consistent performance
- No version conflicts

## Deployment

### Web Server Deployment

1. **Copy entire directory** to web server:
   ```bash
   scp -r data-validation-tool/ user@server:/var/www/html/
   ```

2. **Access via web browser**:
   ```
   http://your-server/data-validation-tool/datak9-studio.html
   ```

### Local File Access

1. **Open directly in browser**:
   ```bash
   # Linux/Mac
   open datak9-studio.html

   # Windows
   start datak9-studio.html
   ```

2. **Or use Python local server**:
   ```bash
   cd data-validation-tool
   python3 -m http.server 8000
   # Open http://localhost:8000/datak9-studio.html
   ```

## Monaco Editor Version

**Current Version:** 0.45.0 (October 2023)

### Updating Monaco

To update to a newer version:

```bash
# 1. Remove old version
rm -rf monaco-editor/

# 2. Download new version (replace X.Y.Z with version)
mkdir -p monaco-editor/min
cd monaco-editor
curl -L "https://registry.npmjs.org/monaco-editor/-/monaco-editor-X.Y.Z.tgz" -o monaco.tgz
tar -xzf monaco.tgz
mv package/min/* .
rm -rf package monaco.tgz

# 3. Move vs/ into min/
mv vs min/

# 4. Update version comment in datak9-studio.html
```

## Features Included

Monaco Editor provides:
- ✅ **Syntax Highlighting** for YAML, JSON, and 80+ languages
- ✅ **IntelliSense** with auto-completion
- ✅ **Error Detection** with squiggly underlines
- ✅ **Code Folding** for nested structures
- ✅ **Find & Replace** with regex support
- ✅ **Multiple Cursors** for bulk editing
- ✅ **Minimap** for navigation
- ✅ **Line Numbers** and formatting
- ✅ **Bracket Matching** and pairing

## Technical Details

### AMD Module Loading

Monaco uses AMD (Asynchronous Module Definition) for loading:

```javascript
// Configure loader
require.config({
    paths: {
        'vs': 'monaco-editor/min/vs'
    }
});

// Load editor
require(['vs/editor/editor.main'], () => {
    const editor = monaco.editor.create(element, options);
});
```

### Language Support

Included languages:
- YAML (primary for DataK9 configurations)
- JSON
- JavaScript
- TypeScript
- Python
- And 75+ more languages

### Browser Compatibility

Tested with:
- ✅ Chrome 90+
- ✅ Firefox 88+
- ✅ Edge 90+
- ✅ Safari 14+

## License

Monaco Editor is licensed under the **MIT License**:
- Free for commercial use
- No attribution required
- Can be modified and redistributed

**Copyright:** © Microsoft Corporation

**DataK9** is also MIT licensed - see [LICENSE](LICENSE) file.

---

## Troubleshooting

### Monaco Not Loading

**Symptom:** Blank YAML editor panel

**Solutions:**
1. Check browser console for errors
2. Verify `monaco-editor/min/vs/loader.js` exists
3. Check file paths are correct (relative to HTML file)
4. Ensure web server serves .js files with correct MIME type

### YAML Syntax Highlighting Not Working

**Solution:** Monaco automatically detects YAML via:
- Language setting: `language: 'yaml'`
- File extension detection
- Manual language mode selection

### Performance Issues

**Recommendations:**
- Use latest browser version
- Ensure adequate RAM (2GB+ available)
- Close unused browser tabs
- Consider chunking very large YAML files (>10,000 lines)

---

## Support

For issues related to:
- **DataK9 Studio:** [GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)
- **Monaco Editor:** [Monaco Editor GitHub](https://github.com/microsoft/monaco-editor)

---

**🐕 DataK9 Studio - Now corporate firewall friendly!**

Author: Daniel Edge
