"""
Local LLM Summarizer for Profile Reports.

Generates AI-powered executive summaries using llama-cpp-python.
Runs entirely locally with no server required - just loads a GGUF model file.

Requirements:
- pip install llama-cpp-python
- A GGUF model file (e.g., from HuggingFace)

Environment Variables:
- DATAK9_LLM_MODEL: Path to GGUF model file

Recommended models (by quality/speed balance):
- Qwen2.5-1.5B-Instruct-GGUF (~1GB) - Best balance
- Qwen2.5-0.5B-Instruct-GGUF (~400MB) - Faster, lower quality
- Phi-3-mini-4k-instruct-GGUF (~2GB) - Best quality, slow

Usage:
    summarizer = LocalLLMSummarizer("/path/to/model.gguf")
    summary = summarizer.generate_summary(profile_data)
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

# Environment variable for model path
LLM_MODEL_ENV = "DATAK9_LLM_MODEL"

# Search paths for GGUF models
GGUF_SEARCH_PATHS = [
    Path.home() / ".cache" / "huggingface" / "hub",
    Path.home() / ".local" / "share" / "models",
    Path.home() / "models",
    Path("/usr/share/models"),
    Path.cwd() / "models",
]

# Preferred model patterns (best balance of quality/speed first)
PREFERRED_PATTERNS = [
    "*qwen*1.5b*.gguf",    # Best balance: ~1GB, 18s gen, good insights
    "*qwen*0.5b*.gguf",    # Faster but lower quality
    "*tinyllama*.gguf",
    "*phi*3*mini*.gguf",   # Best quality but slow (~96s)
    "*llama*3.2*1b*.gguf",
    "*gemma*2b*.gguf",
    "*.gguf",
]


class LocalLLMSummarizer:
    """
    Generates AI-powered summaries using llama-cpp-python.

    No server required - loads GGUF model directly in Python.
    """

    def __init__(self, model_path: Optional[str] = None):
        """
        Initialize the LLM summarizer.

        Args:
            model_path: Path to GGUF model file (or set DATAK9_LLM_MODEL env var)
        """
        self.model_path = model_path or os.environ.get(LLM_MODEL_ENV)
        self._llm = None
        self._available = None
        self._model_name = None

    def _find_model(self) -> Optional[str]:
        """Find a suitable GGUF model file."""
        # Check explicit path first
        if self.model_path and Path(self.model_path).exists():
            return self.model_path

        # Search common locations
        for search_path in GGUF_SEARCH_PATHS:
            if not search_path.exists():
                continue
            for pattern in PREFERRED_PATTERNS:
                matches = list(search_path.rglob(pattern))
                if matches:
                    # Sort by size (prefer smaller models)
                    matches.sort(key=lambda p: p.stat().st_size)
                    logger.info(f"Found GGUF model: {matches[0]}")
                    return str(matches[0])

        return None

    def is_available(self) -> bool:
        """Check if llama-cpp-python is available with a model."""
        if self._available is not None:
            return self._available

        try:
            from llama_cpp import Llama
            model = self._find_model()
            self._available = model is not None
            if not model:
                logger.warning("No GGUF model found. Run: ./scripts/setup-llm.sh")
            return self._available
        except ImportError:
            logger.warning("llama-cpp-python not installed. Run: ./scripts/setup-llm.sh")
            self._available = False
            return False
        except Exception as e:
            logger.debug(f"llama-cpp-python check failed: {e}")
            self._available = False
            return False

    def _load_model(self):
        """Load the model lazily (only when first needed)."""
        if self._llm is not None:
            return self._llm

        model_path = self._find_model()
        if not model_path:
            return None

        try:
            from llama_cpp import Llama

            logger.info(f"Loading GGUF model: {model_path}")
            self._model_name = Path(model_path).stem

            self._llm = Llama(
                model_path=model_path,
                n_ctx=2048,      # Context window
                n_threads=4,     # CPU threads
                n_gpu_layers=0,  # CPU only (set higher for GPU)
                verbose=False,
            )
            return self._llm
        except Exception as e:
            logger.warning(f"Failed to load model: {e}")
            return None

    def _build_prompt(self, profile_data: Dict[str, Any]) -> str:
        """Build a concise prompt for summarization."""
        file_name = profile_data.get('file_name', 'Unknown')
        row_count = profile_data.get('row_count', 0)
        col_count = profile_data.get('column_count', 0)
        quality_score = profile_data.get('overall_quality_score', 0)

        # Column analysis - handle both dict and list formats
        columns = profile_data.get('columns', {})
        high_missing = []
        potential_pii = []

        # Handle columns as dict or list
        if isinstance(columns, dict):
            col_items = columns.items()
        elif isinstance(columns, list):
            col_items = [(c.get('name', ''), c) for c in columns]
        else:
            col_items = []

        for col_name, col_info in col_items:
            missing_pct = col_info.get('missing_percentage', 0) if isinstance(col_info, dict) else 0
            if missing_pct > 20:
                high_missing.append(f"{col_name} ({missing_pct:.0f}%)")
            if isinstance(col_info, dict) and col_info.get('is_potential_pii', False):
                potential_pii.append(col_name)

        # ML findings
        ml_findings = profile_data.get('ml_findings', {})
        ml_summary = ml_findings.get('summary', {})
        outlier_count = ml_summary.get('total_outliers', 0)
        autoencoder_anomalies = ml_summary.get('autoencoder_anomalies', 0)
        benford_suspicious = len([c for c, v in ml_findings.get('benford_analysis', {}).items()
                                  if v.get('suspicious', False)])

        # Correlation findings
        correlations = profile_data.get('correlations', [])
        strong_correlations = []
        for corr in correlations[:5]:  # Top 5
            if isinstance(corr, dict) and abs(corr.get('correlation', 0)) >= 0.5:
                col1 = corr.get('column1', '')
                col2 = corr.get('column2', '')
                val = corr.get('correlation', 0)
                direction = 'positive' if val > 0 else 'negative'
                strong_correlations.append(f"{col1}/{col2} ({direction})")

        # Build issues list with actual data
        issues = []
        if high_missing:
            issues.append(f"Missing data in {', '.join(high_missing[:3])}")
        if outlier_count > 0:
            issues.append(f"{outlier_count} outliers detected")
        if autoencoder_anomalies > 0:
            issues.append(f"{autoencoder_anomalies} anomalous records ({autoencoder_anomalies*100/max(row_count,1):.1f}%)")
        if benford_suspicious > 0:
            issues.append(f"{benford_suspicious} column(s) fail Benford's Law (possible data manipulation)")
        if potential_pii:
            issues.append(f"PII detected: {', '.join(potential_pii[:3])}")

        issues_text = "; ".join(issues) if issues else "No major issues"

        # Correlation insights
        corr_text = ""
        if strong_correlations:
            corr_text = f"\nKey relationships: {', '.join(strong_correlations[:3])}"

        context = f"""Dataset: {file_name} ({row_count:,} rows, {col_count} cols)
Quality: {quality_score:.0f}%
Issues: {issues_text}{corr_text}"""

        return f"""<|im_start|>system
You are a data analyst writing a brief executive summary.<|im_end|>
<|im_start|>user
{context}

In 2-3 sentences: State the main data quality concerns and key relationships found, then give specific recommendations.<|im_end|>
<|im_start|>assistant
"""

    def generate_summary(self, profile_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generate an AI-powered summary."""
        if not self.is_available():
            logger.info("Local LLM not available - skipping AI summary")
            return None

        llm = self._load_model()
        if not llm:
            return None

        try:
            prompt = self._build_prompt(profile_data)

            output = llm(
                prompt,
                max_tokens=150,
                temperature=0.7,
                top_p=0.9,
                repeat_penalty=1.2,
                stop=["<|im_end|>", "\n\n", "###", "---"],
                echo=False,
            )

            summary_text = output['choices'][0]['text'].strip()
            # Clean up any chat template artifacts
            summary_text = summary_text.replace("<|im_end|>", "").strip()
            if not summary_text:
                return None

            return {
                'summary': summary_text,
                'model': self._model_name or 'local-llm',
                'disclaimer': 'AI-generated summary using local LLM. May contain inaccuracies.',
                'generated_locally': True
            }

        except Exception as e:
            logger.warning(f"LLM generation failed: {e}")
            return None

    def generate_summary_html(self, profile_data: Dict[str, Any]) -> str:
        """Generate HTML block for the AI summary section."""
        result = self.generate_summary(profile_data)

        if not result:
            return ""

        summary = result['summary']
        model = result['model']

        return f'''
        <div class="ai-summary-section" style="margin: 20px 0; padding: 20px; background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%); border-radius: 12px; border-left: 4px solid #0ea5e9;">
            <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 12px;">
                <span style="font-size: 1.5em;">🤖</span>
                <h3 style="margin: 0; color: #0369a1; font-size: 1.1em;">AI-Generated Summary</h3>
                <span style="background: #0ea5e9; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.75em; font-weight: 500;">LOCAL LLM</span>
            </div>
            <p style="margin: 0 0 12px 0; color: #334155; line-height: 1.6; font-size: 0.95em;">
                {summary}
            </p>
            <div style="display: flex; align-items: center; gap: 8px; color: #64748b; font-size: 0.8em;">
                <span>⚠️</span>
                <span>AI-generated using <code style="background: #e2e8f0; padding: 1px 4px; border-radius: 3px;">{model}</code> • May contain inaccuracies • Review manually</span>
            </div>
        </div>
        '''


def get_llm_summary_for_report(profile_data: Dict[str, Any]) -> str:
    """
    Convenience function to get LLM summary HTML for a profile report.

    Args:
        profile_data: The profile JSON data

    Returns:
        HTML string for the summary section, or empty string if unavailable
    """
    try:
        summarizer = LocalLLMSummarizer()
        return summarizer.generate_summary_html(profile_data)
    except Exception as e:
        logger.debug(f"LLM summary generation failed: {e}")
        return ""
